package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/chenyao-lib/go/log"
	"github.com/fsnotify/fsnotify"
)

// Callback is called when a watched config field changes.
type Callback func(oldVal, newVal any)

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

	mu           sync.RWMutex
	conf         *T
	isFirstLoad  bool
	watchStarted bool

	cbMu      sync.RWMutex
	callbacks map[string][]Callback
}

var (
	globalCbMu  sync.RWMutex
	globalCbs   = make(map[string][]Callback)
	defaultWait = 200 * time.Millisecond
)

// Init loads filePath config into T and starts config file watching.
func Init[T any](filePath string, decoder Decoder) *T {
	loader := MustNewLoader[T](Options{
		FilePath: filePath,
		Decoder:  decoder,
		Watch:    true,
	})
	return loader.MustLoad()
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
		callbacks:   make(map[string][]Callback),
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

func registerCallback(callbacks map[string][]Callback, mu *sync.RWMutex, path string, cb Callback) {
	if cb == nil {
		return
	}
	key := strings.ToLower(path)
	mu.Lock()
	defer mu.Unlock()
	callbacks[key] = append(callbacks[key], cb)
}

func (l *Loader[T]) triggerCallbacks(path string, oldVal, newVal any) {
	key := strings.ToLower(path)
	callCallbacks(l.callbacks, &l.cbMu, key, oldVal, newVal)
	callCallbacks(globalCbs, &globalCbMu, key, oldVal, newVal)
}

func callCallbacks(callbacks map[string][]Callback, mu *sync.RWMutex, key string, oldVal, newVal any) {
	mu.RLock()
	fns := append([]Callback(nil), callbacks[key]...)
	mu.RUnlock()

	for _, fn := range fns {
		go func(f Callback) {
			defer func() {
				if r := recover(); r != nil {
					log.Error("Recovered from config callback error: %v", r)
				}
			}()
			f(oldVal, newVal)
		}(fn)
	}
}

// Load reads the config file and starts watching it when enabled.
func (l *Loader[T]) Load() (*T, error) {
	if err := l.loadAndProcess(); err != nil {
		return nil, err
	}
	if l.watch {
		if err := l.startWatch(); err != nil {
			return nil, err
		}
		fmt.Println("Load config success, watch config dir: ", l.dir)
	} else {
		fmt.Println("Load config success: ", l.path)
	}
	return l.Get(), nil
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

func (l *Loader[T]) loadAndProcess() error {
	newConf := new(T)
	if err := l.decoder.DecodeFile(l.path, newConf); err != nil {
		return err
	}

	l.mu.Lock()
	oldConf := l.conf
	isFirstLoad := l.isFirstLoad
	l.conf = newConf
	l.isFirstLoad = false
	l.mu.Unlock()

	if !isFirstLoad {
		log.Info("Config change detected (%s):", time.Now().Format("15:04:05"))
		diffConfig(oldConf, newConf, "", l.triggerCallbacks)
	}
	return nil
}

func (l *Loader[T]) startWatch() error {
	l.mu.Lock()
	if l.watchStarted {
		l.mu.Unlock()
		return nil
	}
	l.watchStarted = true
	l.mu.Unlock()

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	if err := watcher.Add(l.dir); err != nil {
		_ = watcher.Close()
		return err
	}

	go func() {
		var (
			timer *time.Timer
			mu    sync.Mutex
		)
		defer watcher.Close()

		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if filepath.Base(event.Name) != l.targetFile {
					continue
				}
				if !isReloadEvent(event.Op) {
					continue
				}

				mu.Lock()
				if timer != nil {
					timer.Stop()
				}
				timer = time.AfterFunc(l.debounce, func() {
					if err := l.loadAndProcess(); err != nil {
						log.Error("Load config error: %v", err)
					} else {
						log.Info("Auto load config success")
					}
				})
				mu.Unlock()

			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Error("Watcher error occurred: %v", err)
			}
		}
	}()
	return nil
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
	oldVal := derefValue(reflect.ValueOf(oldConf))
	newVal := derefValue(reflect.ValueOf(newConf))
	if !oldVal.IsValid() || !newVal.IsValid() {
		return
	}
	if oldVal.Type() != newVal.Type() || oldVal.Kind() != reflect.Struct {
		if !reflect.DeepEqual(valueOf(oldVal), valueOf(newVal)) {
			emit(prefix, valueOf(oldVal), valueOf(newVal))
		}
		return
	}

	oldType := oldVal.Type()
	for i := 0; i < oldVal.NumField(); i++ {
		field := oldType.Field(i)
		if field.PkgPath != "" {
			continue
		}
		name := configFieldName(field)
		if name == "" {
			continue
		}
		path := joinPath(prefix, name)
		v1 := oldVal.Field(i)
		v2 := newVal.Field(i)
		if reflect.DeepEqual(valueOf(v1), valueOf(v2)) {
			continue
		}

		logChange(path, v1, v2)
		emit(path, valueOf(v1), valueOf(v2))

		if canRecurse(v1, v2) {
			diffConfig(valueOf(v1), valueOf(v2), path, emit)
		}
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

func canRecurse(v1, v2 reflect.Value) bool {
	v1 = derefValue(v1)
	v2 = derefValue(v2)
	if !v1.IsValid() || !v2.IsValid() || v1.Type() != v2.Type() {
		return false
	}
	if v1.Kind() != reflect.Struct {
		return false
	}
	return v1.Type() != reflect.TypeOf(time.Time{})
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

func logChange(path string, oldVal, newVal reflect.Value) {
	oldEmpty := isZero(oldVal)
	newEmpty := isZero(newVal)
	oldAny := valueOf(oldVal)
	newAny := valueOf(newVal)

	if oldEmpty && !newEmpty {
		log.Info("[ADD] %s = %v", path, newAny)
		return
	}
	if !oldEmpty && newEmpty {
		log.Info("[DELETE] %s: remove (original %v)", path, oldAny)
		return
	}
	log.Info("[MODIFY] %s: %v -> %v", path, oldAny, newAny)
}

func isZero(v reflect.Value) bool {
	if !v.IsValid() {
		return true
	}
	return reflect.DeepEqual(valueOf(v), reflect.Zero(v.Type()).Interface())
}
