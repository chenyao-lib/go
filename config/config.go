// Package config 提供泛型配置加载、文件查找、热更新、字段变更回调和简单的
// 进程级配置存取。解析格式由调用方通过 Decoder 注入，因此可用于 JSON、
// YAML、TOML 或其他格式。
//
// # 快速加载
//
// 使用 DecoderFunc 可以直接接入标准库 JSON：
//
//	type AppConfig struct {
//		Server struct {
//			Addr string `json:"addr"`
//		} `json:"server"`
//	}
//
//	decoder := config.DecoderFunc(func(path string, v any) error {
//		data, err := os.ReadFile(path)
//		if err != nil {
//			return err
//		}
//		return json.Unmarshal(data, v)
//	})
//
//	cfg := config.Init[AppConfig]("./app.json", decoder)
//	log.Info("server addr: %s", cfg.Server.Addr)
//
// Init 会立即加载指定文件、开启目录监听，并通过 Store 注册初始配置，使其他
// 包可用 config.Get[AppConfig]() 获取。LoadFile 只加载一次，不启动监听。
//
// # 独立 Loader
//
// 需要控制文件查找、监听开关或防抖时间时使用 Loader：
//
//	loader := config.MustNewLoader[AppConfig](config.Options{
//		AppName:    "gateway",
//		Extensions: []string{".json"},
//		Decoder:    decoder,
//		Watch:      true,
//		Debounce:   500 * time.Millisecond,
//	})
//	defer loader.Close()
//	loader.OnChange("server.addr", func(oldValue, newValue any) {
//		log.Info("addr: %v -> %v", oldValue, newValue)
//	})
//	cfg := loader.MustLoad()
//
// FilePath 指定精确路径；FileName 会依次在可执行文件目录和工作目录查找；
// AppName+Extensions 会按扩展名依次查找。三种方式只需选择一种。
//
// 热更新时 Loader 会替换内部配置指针，因此长期运行的代码应调用 loader.Get()
// 读取最新快照，不要永久保存首次 Load 返回的指针。最近一次显式加载的监听型
// Loader 更新包级 Get 的快照，其他 Loader 仍可独立热更新。
//
// # 变更回调
//
// 字段路径不区分大小写，路径名依次取 toml、yaml、json 标签，最后才使用 Go
// 字段名。嵌套字段用点连接，例如 server.addr。Loader.OnChange 只作用于该
// Loader；包级 OnChange 作用于所有 Loader。每个注册的回调异步、串行执行，
// 不同注册可以并发。积压的同一 Loader 更新合并为最早的 old 和最新的 new，
// 包级回调跨 Loader 的积压保留最近一条变更。
//
// 回调只针对叶子字段：有公开字段的结构体会递归到 server.addr 这类具体路径。
// 可选结构体不存在时，其子字段在回调中的值为 nil。
// netip.Addr 等没有公开字段的结构体作为叶子值上报。
// 变更日志只输出字段路径，字段值通过回调传递。
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenyao-lib/go/log"
	"github.com/fsnotify/fsnotify"
)

// Callback is called when a watched config field changes.
type Callback func(oldVal, newVal any)

// ErrLoaderClosed is returned when Load is called after Close.
var ErrLoaderClosed = errors.New("config loader is closed")

// Decoder decodes a config file into v.
//
// The config package does not import TOML/YAML/JSON parsers directly. Keep it
// generic by creating a small adapter in the caller package and passing it to
// Init, LoadFile, or NewLoader.
//
// TOML example:
//
//	import "github.com/BurntSushi/toml"
//
//	type tomlDecoder struct{}
//
//	func (tomlDecoder) DecodeFile(path string, v any) error {
//		_, err := toml.DecodeFile(path, v)
//		return err
//	}
//
//	conf := config.Init[AppConfig]("./myapp.toml", tomlDecoder{})
//
// YAML example:
//
//	import (
//		"os"
//		"gopkg.in/yaml.v3"
//	)
//
//	type yamlDecoder struct{}
//
//	func (yamlDecoder) DecodeFile(path string, v any) error {
//		data, err := os.ReadFile(path)
//		if err != nil {
//			return err
//		}
//		return yaml.Unmarshal(data, v)
//	}
//
//	conf := config.Init[AppConfig]("./myapp.yaml", yamlDecoder{})
//
// JSON example, using only the standard library:
//
//	import (
//		"encoding/json"
//		"os"
//	)
//
//	type jsonDecoder struct{}
//
//	func (jsonDecoder) DecodeFile(path string, v any) error {
//		data, err := os.ReadFile(path)
//		if err != nil {
//			return err
//		}
//		return json.Unmarshal(data, v)
//	}
//
//	conf := config.Init[AppConfig]("./myapp.json", jsonDecoder{})
//
// Advanced example, when you need FilePath, FileName, Debounce, or Watch=false:
//
//	loader := config.MustNewLoader[AppConfig](config.Options{
//		FilePath: "./myapp.json",
//		Decoder:  jsonDecoder{},
//		Debounce: 500 * time.Millisecond,
//		Watch:    true,
//	})
//	conf := loader.MustLoad()
//
// File lookup:
//   - FilePath loads exactly that file.
//   - FileName searches executable dir, then working dir, for that file name.
//   - AppName + Extensions searches executable dir, then working dir, for
//     names such as myapp.toml, myapp.yaml, or myapp.json.
//
// Change callbacks use struct tag names in this order: toml, yaml, json, field
// name. For example OnChange("server.wsport", fn) works when the field has
// `toml:"server"` / `yaml:"server"` tags.
type Decoder interface {
	DecodeFile(path string, v any) error
}

type DecoderFunc func(path string, v any) error

func (f DecoderFunc) DecodeFile(path string, v any) error {
	return f(path, v)
}

// Options controls how a config file is located and watched.
type Options struct {
	AppName    string
	FileName   string
	FilePath   string
	Extensions []string
	Watch      bool
	Debounce   time.Duration
	Decoder    Decoder
}

// Loader loads config into any caller-owned struct type.
type Loader[T any] struct {
	path       string
	dir        string
	targetFile string
	watch      bool
	debounce   time.Duration
	decoder    Decoder

	loadMu   sync.Mutex
	watcher  *fileWatch
	closed   bool
	closeErr error

	mu          sync.RWMutex
	conf        *T
	isFirstLoad bool

	cbMu      sync.RWMutex
	callbacks map[string][]*callbackSubscription
}

type fileWatch struct {
	watcher *fsnotify.Watcher
	dir     os.FileInfo
	stop    chan struct{}
	done    chan error
	failed  atomic.Bool
}

const watchPollInterval = 200 * time.Millisecond

func (w *fileWatch) watches(dir os.FileInfo) bool {
	return !w.failed.Load() && os.SameFile(w.dir, dir) && len(w.watcher.WatchList()) != 0
}

func (w *fileWatch) close() error {
	close(w.stop)
	return <-w.done
}

var (
	globalCbMu  sync.RWMutex
	globalCbs   = make(map[string][]*callbackSubscription)
	defaultWait = 200 * time.Millisecond

	globalMu     sync.RWMutex
	globalConf   any
	globalSource any // Loader pointer that may publish hot reloads to globalConf

	initMu     sync.Mutex
	initLoader interface{ Close() error }
)

// Init loads filePath config into T and starts config file watching.
// The loaded config is also registered globally so that other packages can
// retrieve it later with Get[T]().
// Repeated calls for the same absolute path and type reuse the Loader.
// A successful call for a different config closes the previous Init watcher.
func Init[T any](filePath string, decoder Decoder) *T {
	loader := MustNewLoader[T](Options{
		FilePath: filePath,
		Decoder:  decoder,
		Watch:    true,
	})
	initMu.Lock()
	defer initMu.Unlock()
	previous := initLoader
	if current, ok := previous.(*Loader[T]); ok && current.path == loader.path {
		loader = current
	}
	conf, err := loader.load(decoder)
	if err != nil {
		panic(err)
	}
	initLoader = loader
	if previous != nil && previous != loader {
		if err := previous.Close(); err != nil {
			log.Warn("关闭旧配置监听失败: %v", err)
		}
	}
	return conf
}

// Store registers a configuration snapshot for Get[T]() and supersedes the
// current watcher as the global source. Init and watched Load publish automatically.
func Store[T any](conf *T) {
	globalMu.Lock()
	globalConf = conf
	globalSource = nil
	globalMu.Unlock()
}

// Get returns the globally registered config of type T. Returns nil if no
// config has been registered (via Init, Store, etc.) or the type does not
// match.
//
// Usage:
//
//	cfg := config.Get[AppConfig]()
//	if cfg == nil {
//	    // not initialised
//	}
//	_ = cfg.SomeField
func Get[T any]() *T {
	globalMu.RLock()
	defer globalMu.RUnlock()
	conf, ok := globalConf.(*T)
	if !ok {
		return nil
	}
	return conf
}

// LoadFile loads a specific config file once without watching it.
func LoadFile[T any](filePath string, decoder Decoder) (*T, error) {
	loader, err := NewLoader[T](Options{FilePath: filePath, Decoder: decoder})
	if err != nil {
		return nil, err
	}
	return loader.Load()
}

// NewLoader creates a reusable loader for a caller-owned config type.
func NewLoader[T any](opts Options) (*Loader[T], error) {
	if opts.Decoder == nil {
		return nil, fmt.Errorf("config decoder is required")
	}

	path, dir, targetFile, err := resolvePath(opts)
	if err != nil {
		return nil, err
	}

	debounce := opts.Debounce
	if debounce <= 0 {
		debounce = defaultWait
	}

	return &Loader[T]{
		path:        path,
		dir:         dir,
		targetFile:  targetFile,
		watch:       opts.Watch,
		debounce:    debounce,
		decoder:     opts.Decoder,
		isFirstLoad: true,
		callbacks:   make(map[string][]*callbackSubscription),
	}, nil
}

func MustNewLoader[T any](opts Options) *Loader[T] {
	loader, err := NewLoader[T](opts)
	if err != nil {
		panic(err)
	}
	return loader
}

// OnChange registers a package-level callback. It is kept for simple apps.
func OnChange(path string, cb Callback) {
	registerCallback(globalCbs, &globalCbMu, path, cb)
}

// OnChange registers a callback for this loader only.
func (l *Loader[T]) OnChange(path string, cb Callback) {
	registerCallback(l.callbacks, &l.cbMu, path, cb)
}

func registerCallback(callbacks map[string][]*callbackSubscription, mu *sync.RWMutex, path string, cb Callback) {
	if cb == nil {
		return
	}
	key := strings.ToLower(path)
	mu.Lock()
	defer mu.Unlock()
	callbacks[key] = append(callbacks[key], &callbackSubscription{callback: cb})
}

func (l *Loader[T]) triggerCallbacks(path string, oldVal, newVal any) {
	key := strings.ToLower(path)
	update := callbackUpdate{source: l, oldVal: oldVal, newVal: newVal}
	callCallbacks(l.callbacks, &l.cbMu, key, update)
	callCallbacks(globalCbs, &globalCbMu, key, update)
}

func callCallbacks(callbacks map[string][]*callbackSubscription, mu *sync.RWMutex, key string, update callbackUpdate) {
	mu.RLock()
	subscriptions := append([]*callbackSubscription(nil), callbacks[key]...)
	mu.RUnlock()

	for _, subscription := range subscriptions {
		subscription.enqueue(key, update)
	}
}

type callbackUpdate struct {
	source any // the Loader pointer that produced the update
	oldVal any
	newVal any
}

type callbackSubscription struct {
	callback Callback
	mu       sync.Mutex
	running  bool
	pending  *callbackUpdate
}

func (s *callbackSubscription) enqueue(key string, update callbackUpdate) {
	s.mu.Lock()
	if s.running {
		switch {
		case s.pending == nil:
			s.pending = &update
		case s.pending.source == update.source:
			s.pending.newVal = update.newVal
		default:
			*s.pending = update
		}
		s.mu.Unlock()
		return
	}
	s.running = true
	s.mu.Unlock()
	go s.run(key, update)
}

func (s *callbackSubscription) run(key string, update callbackUpdate) {
	for {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Error("配置变更回调 panic: path=%s, panic=%v", key, r)
				}
			}()
			s.callback(update.oldVal, update.newVal)
		}()

		s.mu.Lock()
		if s.pending == nil {
			s.running = false
			s.mu.Unlock()
			return
		}
		update = *s.pending
		s.pending = nil
		s.mu.Unlock()
	}
}

// Load reads the config file and starts watching it when enabled.
func (l *Loader[T]) Load() (*T, error) {
	return l.load(nil)
}

func (l *Loader[T]) load(decoder Decoder) (*T, error) {
	l.loadMu.Lock()
	defer l.loadMu.Unlock()
	if l.closed {
		return nil, ErrLoaderClosed
	}

	previous := l.watcher
	if l.watch {
		if err := l.startWatchLocked(); err != nil {
			log.Error("配置监听启动失败: path=%s, dir=%s, err=%v", l.path, l.dir, err)
			return nil, err
		}
		if l.isFirstLoad && l.watcher != previous {
			defer func() {
				if l.isFirstLoad {
					_ = l.watcher.close()
					l.watcher = nil
				}
			}()
		}
	}
	if err := l.loadAndProcessLocked(decoder, true); err != nil {
		log.Error("配置加载失败: path=%s, err=%v", l.path, err)
		return nil, err
	}
	if l.watch {
		log.Info("配置加载成功，已启动监听: path=%s, dir=%s", l.path, l.dir)
	} else {
		log.Info("配置加载成功: path=%s", l.path)
	}
	return l.Get(), nil
}

// Close stops watching and prevents further loads. Get retains the last snapshot.
// Callbacks already scheduled before Close may still run.
func (l *Loader[T]) Close() error {
	l.loadMu.Lock()
	defer l.loadMu.Unlock()
	if !l.closed {
		l.closed = true
		if watcher := l.watcher; watcher != nil {
			l.watcher = nil
			l.closeErr = watcher.close()
		}
		globalMu.Lock()
		if globalSource == l {
			globalSource = nil
		}
		globalMu.Unlock()
	}
	return l.closeErr
}

func (l *Loader[T]) MustLoad() *T {
	conf, err := l.Load()
	if err != nil {
		panic(err)
	}
	return conf
}

func (l *Loader[T]) Get() *T {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.conf
}

func (l *Loader[T]) Path() string {
	return l.path
}

func (l *Loader[T]) loadAndProcessLocked(decoder Decoder, explicit bool) error {
	if decoder == nil {
		decoder = l.decoder
	}
	newConf := new(T)
	if err := decodeFile(decoder, l.path, newConf); err != nil {
		return err
	}
	l.decoder = decoder

	l.mu.Lock()
	oldConf := l.conf
	isFirstLoad := l.isFirstLoad
	l.conf = newConf
	l.isFirstLoad = false
	l.mu.Unlock()

	if l.watch {
		globalMu.Lock()
		if explicit || globalSource == l {
			globalSource, globalConf = l, newConf
		}
		globalMu.Unlock()
	}
	if !isFirstLoad {
		log.Info("检测到配置变化: path=%s, time=%s", l.path, time.Now().Format("15:04:05"))
		diffConfig(oldConf, newConf, "", l.triggerCallbacks)
	}
	return nil
}

func decodeFile(decoder Decoder, path string, dst any) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("config decoder panic: %v", value)
		}
	}()
	return decoder.DecodeFile(path, dst)
}

func (l *Loader[T]) startWatchLocked() error {
	dir, err := statWatchPath(l.dir)
	if err != nil {
		return err
	}
	previous := l.watcher
	if previous != nil && previous.watches(dir) {
		return nil
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	if err := watcher.Add(l.dir); err != nil {
		_ = watcher.Close()
		return err
	}
	currentDir, err := statWatchPath(l.dir)
	if err != nil {
		_ = watcher.Close()
		return err
	}
	if !os.SameFile(dir, currentDir) {
		_ = watcher.Close()
		return fmt.Errorf("config directory changed while starting watcher: %s", l.dir)
	}

	current := &fileWatch{watcher: watcher, dir: dir, stop: make(chan struct{}), done: make(chan error, 1)}
	l.watcher = current
	resolvedPath, targetInfo := l.resolveWatchTarget()
	go l.watchFiles(current, resolvedPath, targetInfo)
	if previous != nil {
		_ = previous.close()
	}
	return nil
}

func statWatchPath(path string) (os.FileInfo, error) {
	if runtime.GOOS != "windows" {
		return os.Stat(path)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	// On Windows, File.Stat captures the identity from the open handle.
	return file.Stat()
}

func (l *Loader[T]) resolveWatchTarget() (string, os.FileInfo) {
	path, err := filepath.EvalSymlinks(l.path)
	if err != nil {
		return "", nil
	}
	if filepath.Dir(path) == l.dir {
		return path, nil // The directory watcher already sees writes here.
	}
	info, err := statWatchPath(path)
	if err != nil {
		return "", nil
	}
	return path, info
}

func (l *Loader[T]) watchFiles(watch *fileWatch, resolvedPath string, targetInfo os.FileInfo) {
	var timer *time.Timer
	var timerReady <-chan time.Time
	var reloadDone <-chan struct{}
	ticker := time.NewTicker(watchPollInterval)
	events, watchErrors := watch.watcher.Events, watch.watcher.Errors
	defer func() {
		ticker.Stop()
		if timer != nil {
			timer.Stop()
		}
		watch.done <- watch.watcher.Close()
	}()

	refreshTarget := func() bool {
		path, info := l.resolveWatchTarget()
		if path == "" {
			resolvedPath, targetInfo = "", nil
			return false
		}
		changed := path != resolvedPath
		if !changed && info != nil {
			changed = targetInfo == nil || !os.SameFile(targetInfo, info) ||
				targetInfo.Size() != info.Size() || !targetInfo.ModTime().Equal(info.ModTime())
		}
		resolvedPath, targetInfo = path, info
		return changed
	}
	scheduleReload := func() {
		if timer == nil {
			timer = time.NewTimer(l.debounce)
		} else {
			timer.Reset(l.debounce)
		}
		timerReady = timer.C
	}

	for {
		// Keep collecting changes while decoding. A single timer holds the
		// latest debounce deadline until the running reload finishes.
		reloadReady := timerReady
		if reloadDone != nil {
			reloadReady = nil
		}
		select {
		case <-watch.stop:
			return
		case <-reloadReady:
			timerReady = nil
			done := make(chan struct{})
			reloadDone = done
			go func() {
				defer close(done)
				l.reloadWatched(watch)
			}()
		case <-reloadDone:
			reloadDone = nil
		case <-ticker.C:
			// A replaced directory may leave fsnotify attached to its old inode.
			// An unresolved path may be a dangling link whose target is created elsewhere.
			if dir, err := statWatchPath(l.dir); err == nil && !watch.watches(dir) {
				if timerReady == nil {
					scheduleReload()
				}
			} else if resolvedPath != l.path && refreshTarget() {
				scheduleReload()
			}
		case event, ok := <-events:
			if !ok {
				watch.failed.Store(true)
				events = nil
				continue
			}
			if !isReloadEvent(event.Op) {
				continue
			}
			if filepath.Base(event.Name) == l.targetFile || filepath.Clean(event.Name) == resolvedPath {
				refreshTarget()
				scheduleReload()
			} else if resolvedPath != l.path && refreshTarget() {
				scheduleReload()
			}

		case err, ok := <-watchErrors:
			if !ok {
				watch.failed.Store(true)
				watchErrors = nil
				continue
			}
			log.Error("文件监听异常: path=%s, err=%v", l.path, err)
			if errors.Is(err, fsnotify.ErrEventOverflow) {
				refreshTarget()
				if timerReady == nil {
					scheduleReload()
				}
			}
		}
	}
}

func (l *Loader[T]) reloadWatched(watch *fileWatch) {
	l.loadMu.Lock()
	defer l.loadMu.Unlock()
	if l.watcher != watch {
		return
	}
	if err := l.startWatchLocked(); err != nil {
		log.Warn("恢复配置监听失败: dir=%s, err=%v", l.dir, err)
		return
	}
	if err := l.loadAndProcessLocked(nil, false); err != nil {
		log.Error("自动加载配置失败: path=%s, err=%v", l.path, err)
	} else {
		log.Info("自动加载配置成功: path=%s", l.path)
	}
}

func isReloadEvent(op fsnotify.Op) bool {
	return op&fsnotify.Write == fsnotify.Write ||
		op&fsnotify.Create == fsnotify.Create ||
		op&fsnotify.Rename == fsnotify.Rename
}

func resolvePath(opts Options) (string, string, string, error) {
	if opts.FilePath != "" {
		abs, err := filepath.Abs(opts.FilePath)
		if err != nil {
			return "", "", "", err
		}
		if _, err := os.Stat(abs); err != nil {
			return "", "", "", err
		}
		return abs, filepath.Dir(abs), filepath.Base(abs), nil
	}

	exePath, err := os.Executable()
	if err != nil {
		return "", "", "", err
	}
	exeDir := filepath.Dir(exePath)
	exeName := filepath.Base(exePath)
	workingDir, err := os.Getwd()
	if err != nil {
		return "", "", "", err
	}

	fileNames, err := configFileNames(opts.FileName, opts.Extensions, exeName, opts.AppName)
	if err != nil {
		return "", "", "", err
	}
	dirs := uniqueNonEmpty(exeDir, workingDir)
	tried := make([]string, 0, len(fileNames)*len(dirs))
	for _, dir := range dirs {
		for _, name := range fileNames {
			candidate := filepath.Join(dir, name)
			tried = append(tried, candidate)
			if _, err := os.Stat(candidate); err == nil {
				return candidate, dir, name, nil
			}
		}
	}
	return "", "", "", fmt.Errorf("config file not found, tried: %s", strings.Join(tried, ", "))
}

func configFileNames(explicitName string, extensions []string, bases ...string) ([]string, error) {
	if explicitName != "" {
		return []string{explicitName}, nil
	}

	exts := normalizeExtensions(extensions)
	if len(exts) == 0 {
		return nil, fmt.Errorf("config extensions are required when FileName and FilePath are empty")
	}

	names := make([]string, 0, len(bases)*len(exts))
	for _, base := range uniqueNonEmpty(bases...) {
		for _, ext := range exts {
			names = append(names, base+ext)
		}
	}
	return uniqueNonEmpty(names...), nil
}

func normalizeExtensions(extensions []string) []string {
	result := make([]string, 0, len(extensions))
	for _, ext := range extensions {
		ext = strings.TrimSpace(ext)
		if ext == "" {
			continue
		}
		if !strings.HasPrefix(ext, ".") {
			ext = "." + ext
		}
		result = append(result, ext)
	}
	return uniqueNonEmpty(result...)
}

func uniqueNonEmpty(values ...string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func diffConfig(oldConf, newConf any, prefix string, emit func(path string, oldVal, newVal any)) {
	diffConfigValues(derefValue(reflect.ValueOf(oldConf)), derefValue(reflect.ValueOf(newConf)),
		prefix, emit, make(map[[2]reflect.Value]bool))
}

func diffConfigValues(oldVal, newVal reflect.Value, prefix string, emit func(string, any, any), visiting map[[2]reflect.Value]bool) {
	before, after := valueOf(oldVal), valueOf(newVal)
	if reflect.DeepEqual(before, after) {
		return
	}
	oldStruct, newStruct := derefValue(oldVal), derefValue(newVal)
	if !oldStruct.IsValid() && !newStruct.IsValid() {
		return
	}
	typ := configStructType(oldStruct, newStruct)
	if typ == nil {
		if prefix != "" {
			log.Info("配置字段已更新: %s", prefix)
		}
		emit(prefix, before, after)
		return
	}

	// Decoders may return recursive pointers. Track only the current path so
	// shared values reachable through another field still get reported there.
	pair := [2]reflect.Value{oldStruct, newStruct}
	if visiting[pair] {
		return
	}
	visiting[pair] = true
	defer delete(visiting, pair)

	for i := range typ.NumField() {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}
		name := configFieldName(field)
		if name == "" {
			continue
		}
		var oldField, newField reflect.Value
		if oldStruct.IsValid() {
			oldField = oldStruct.Field(i)
		}
		if newStruct.IsValid() {
			newField = newStruct.Field(i)
		}
		diffConfigValues(oldField, newField, joinPath(prefix, name), emit, visiting)
	}
}

func configFieldName(field reflect.StructField) string {
	for _, tagName := range []string{"toml", "yaml", "json"} {
		tag := field.Tag.Get(tagName)
		if tag == "" {
			continue
		}
		name := strings.Split(tag, ",")[0]
		if name == "-" {
			return ""
		}
		if name != "" {
			return name
		}
	}
	return field.Name
}

func joinPath(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "." + name
}

func configStructType(v1, v2 reflect.Value) reflect.Type {
	if !v1.IsValid() {
		v1 = v2
	}
	if v1.IsValid() && (!v2.IsValid() || v1.Type() == v2.Type()) && hasConfigFields(v1.Type()) {
		return v1.Type()
	}
	return nil
}

func hasConfigFields(typ reflect.Type) bool {
	if typ.Kind() != reflect.Struct {
		return false
	}
	for i := range typ.NumField() {
		if typ.Field(i).IsExported() {
			return true
		}
	}
	return false
}

func derefValue(v reflect.Value) reflect.Value {
	for v.IsValid() && (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) {
		if v.IsNil() {
			return reflect.Value{}
		}
		v = v.Elem()
	}
	return v
}

func valueOf(v reflect.Value) any {
	if !v.IsValid() {
		return nil
	}
	if v.CanInterface() {
		return v.Interface()
	}
	return nil
}
