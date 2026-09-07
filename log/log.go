// Package log 提供进程级日志输出，支持按天/大小滚动文件、控制台输出、运行时级别控制、
// 缓冲写入以及备份文件的自动清理（按数量或保留时间）。
//
// # 特性
//   - 零配置启动：即使不调用 Init，也会使用默认配置（./logs 目录、100MB 滚动、DEBUG 级别）。
//   - 非阻塞写入：所有日志写入操作均通过通道发送到后台协程，用户协程永不阻塞。
//   - 并发安全：所有公开函数均可安全地在多个 goroutine 中同时调用。
//   - 动态配置：可在运行时调整级别、保留策略、缓冲参数等，无需重启。
//   - 优雅关闭：Close 会等待所有已入队日志写入完成，确保数据不丢失。
//
// # 基本用法
//
//	func main() {
//		// 可选：自定义配置
//		log.Init("gateway", "./logs", 100*1024*1024, log.LevelInfo, 10, 7*24*time.Hour)
//		defer log.Close()
//
//		log.Info("server started at %s", ":8080")
//	}
//
// 即使不调用 Init，日志也会默认写入 ./logs/log_YYYYMMDD.log，级别为 DEBUG。
//
// 该包经过并发安全优化，所有配置字段通过 atomic.Value 存储，消除数据竞争；
// 备份文件清理使用正则匹配，避免前缀含下划线导致解析失效。
package log

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/outrigdev/goid"
)

// Level 定义日志级别，数值越小级别越低。
type Level int

const (
	LevelDebug Level = iota // 调试信息
	LevelInfo               // 常规信息
	LevelWarn               // 警告
	LevelError              // 错误（会附带调用栈）
)

const (
	defaultPrefix     = "log"
	defaultDir        = "logs"
	defaultMaxSize    = 100 * 1024 * 1024 // 100MB
	defaultBufSize    = 64 * 1024         // 64KB
	defaultFlushIntvl = 5 * time.Second
	defaultChanCap    = 10000 // 通道容量，最多缓存 10000 条未处理日志
	timestampLayout   = "2006-01-02 15:04:05.000000000"
)

// config 包含所有可动态调整的配置参数，通过 atomic.Value 原子存储指针，实现无锁读取。
type config struct {
	prefix        string         // 日志文件名前缀
	dir           string         // 日志存储目录
	maxSize       int64          // 单个日志文件最大字节数
	maxBackups    int            // 保留的备份文件数（不含当前日志），0 表示不限制
	maxAge        time.Duration  // 备份文件保留的最大时间，0 表示不限制
	console       bool           // 是否同时输出到控制台
	location      *time.Location // 时间戳时区
	bufSize       int            // 缓冲区大小（字节），0 表示不缓冲
	flushInterval time.Duration  // 定时刷新间隔，0 表示禁用定时刷新
}

// Logger 全局日志实例（包内单例）。
// 所有字段在后台协程中操作，用户协程通过通道发送消息，无需加锁。
type Logger struct {
	mu     sync.Mutex   // 保护配置变更和文件操作（reopen, logFile, bufw, flushTicker 等）
	level  atomic.Int32 // 日志级别，独立原子操作
	config atomic.Value // 存储 *config，所有读配置的地方均通过 Load 获取

	// 以下字段仅在后台协程中访问，或受 mu 保护
	logFile     *os.File
	logTime     time.Time
	bufw        *bufio.Writer
	flushTicker *time.Ticker
	flushDone   chan struct{}
	reopen      bool // 标记是否需要重新打开文件（配置变更时置 true）

	// 异步通道
	logCh chan string
	wg    sync.WaitGroup
}

// std 是包级默认实例，在 init 中初始化并启动后台协程。
var std = &Logger{
	logCh: make(chan string, defaultChanCap),
}

// init 在包加载时自动设置默认配置并启动后台写入协程，实现零配置可用。
func init() {
	// 存储默认配置
	std.config.Store(&config{
		prefix:        defaultPrefix,
		dir:           defaultDir,
		maxSize:       defaultMaxSize,
		maxBackups:    0,
		maxAge:        0,
		console:       true,
		location:      time.Local,
		bufSize:       defaultBufSize,
		flushInterval: defaultFlushIntvl,
	})
	std.level.Store(int32(LevelDebug))

	std.wg.Add(1)
	go std.backgroundWriter()
}

// ==================== 公开配置函数 ====================

// Init 初始化日志配置。可多次调用以动态调整参数，后台协程会自动应用新配置。
//
// 参数：
//   - prefix: 日志文件名前缀（不含扩展名），会自动过滤非法字符。若为空则使用 "log"。
//   - dir: 日志存储目录，若为空则使用 "./logs"。目录不存在时会自动创建。
//   - maxSize: 单个日志文件的最大字节数，超过后触发滚动。<=0 时使用默认 100MB。
//   - lv: 最低输出级别，低于此级别的日志将被忽略。
//   - maxBackups: 保留的备份文件数量（不含当前日志），0 表示不限制。
//   - maxAge: 备份文件的最大保留时间（如 7*24*time.Hour），0 表示不限制。
//
// 注意：若调用时已有文件打开，会平滑切换到新配置（关闭旧文件，创建新文件），
// 切换过程中已入队的日志不会丢失。
func Init(prefix, dir string, maxSize int64, lv Level, maxBackups int, maxAge time.Duration) {
	prefix = sanitizePrefix(prefix)
	if dir == "" {
		dir = defaultDir
	}
	if maxSize <= 0 {
		maxSize = defaultMaxSize
	}
	if maxBackups < 0 {
		maxBackups = 0
	}
	if maxAge < 0 {
		maxAge = 0
	}

	std.mu.Lock()
	defer std.mu.Unlock()

	// 获取旧配置，复制并修改
	oldCfg := std.config.Load().(*config)
	newCfg := *oldCfg
	newCfg.prefix = prefix
	newCfg.dir = dir
	newCfg.maxSize = maxSize
	newCfg.maxBackups = maxBackups
	newCfg.maxAge = maxAge
	std.config.Store(&newCfg)

	std.level.Store(int32(lv))

	// 标记需要重新打开文件（因为 dir 或 prefix 可能变化）
	std.reopen = true

	// 立即执行一次清理（使用新配置）
	std.cleanupOldFiles(time.Now())
}

// SetRetention 动态调整备份文件的保留策略。
//
// 参数：
//   - maxBackups: 保留的备份文件数量上限（不含当前日志），0 表示不限制。
//   - maxAge: 备份文件的最大保留时间（如 7*24*time.Hour），0 表示不限制。
//
// 调整后会立即执行一次清理，删除超出限制的旧文件。
func SetRetention(maxBackups int, maxAge time.Duration) {
	std.mu.Lock()
	defer std.mu.Unlock()

	if maxBackups < 0 {
		maxBackups = 0
	}
	if maxAge < 0 {
		maxAge = 0
	}

	oldCfg := std.config.Load().(*config)
	newCfg := *oldCfg
	newCfg.maxBackups = maxBackups
	newCfg.maxAge = maxAge
	std.config.Store(&newCfg)

	std.cleanupOldFiles(time.Now())
}

// SetBufferConfig 设置缓冲参数，调整写入性能和实时性。
//
// 参数：
//   - bufSize: 缓冲区大小（字节）。>0 时启用缓冲，<=0 时不缓冲（直接写文件）。
//   - flushInterval: 定时刷新间隔。>0 时启用定时刷新，<=0 则禁用定时刷新（仅在缓冲区满时刷新）。
//
// 注意：此函数会立即重建缓冲区（如果文件已打开），下一条日志将使用新缓冲设置。
func SetBufferConfig(bufSize int, flushInterval time.Duration) {
	std.mu.Lock()
	defer std.mu.Unlock()

	oldCfg := std.config.Load().(*config)
	newCfg := *oldCfg
	if bufSize > 0 {
		newCfg.bufSize = bufSize
	} else {
		newCfg.bufSize = 0
	}
	if flushInterval > 0 {
		newCfg.flushInterval = flushInterval
	} else {
		newCfg.flushInterval = 0
	}
	std.config.Store(&newCfg)

	// 如果文件已打开，立即重建刷新器（并重建 buffer，在后台协程中会应用）
	if std.logFile != nil {
		std.stopFlusher()
		if newCfg.flushInterval > 0 {
			std.startFlusher()
		}
		// 重建 bufw 由后台协程在下次写入时完成（需设置 reopen 或直接处理？）
		// 但为了立即生效，我们直接重建 bufw，因为持有锁且文件已打开
		if std.bufw != nil {
			std.bufw.Flush()
		}
		if newCfg.bufSize > 0 {
			std.bufw = bufio.NewWriterSize(std.logFile, newCfg.bufSize)
		} else {
			std.bufw = nil
		}
	}
}

// SetLevel 动态调整最低输出级别。低于此级别的日志将被忽略。
//
// 示例：
//
//	log.SetLevel(log.LevelWarn) // 只输出 WARN 及以上级别
func SetLevel(lv Level) {
	std.level.Store(int32(lv))
}

// SetConsole 设置是否同时将日志输出到控制台（stdout）。
//
// 默认开启（true）。关闭后只写入文件。
func SetConsole(enable bool) {
	std.mu.Lock()
	defer std.mu.Unlock()

	oldCfg := std.config.Load().(*config)
	newCfg := *oldCfg
	newCfg.console = enable
	std.config.Store(&newCfg)
}

// SetTimezone 设置日志时间戳所使用的时区。
//
// 参数 name 可以是 "Local"（服务器本地时区）、"Asia/Shanghai" 等 IANA 时区名称。
// 若 name 为空或 "Local"，则使用 time.Local。
// 成功返回 nil，否则返回错误。
//
// 示例：
//
//	if err := log.SetTimezone("Asia/Shanghai"); err != nil {
//		panic(err)
//	}
func SetTimezone(name string) error {
	location, err := resolveTimezone(name)
	if err != nil {
		return err
	}

	std.mu.Lock()
	defer std.mu.Unlock()

	oldCfg := std.config.Load().(*config)
	newCfg := *oldCfg
	newCfg.location = location
	std.config.Store(&newCfg)
	return nil
}

// Close 关闭日志系统，刷新所有缓冲区，并等待后台协程将所有已入队日志写入完成。
//
// 应在程序退出前调用（通常使用 defer）。调用后不能再写入日志。
//
// 注意：如果程序异常退出（如 panic）未调用 Close，可能会丢失少量缓冲中的日志，
// 但已发送到通道的消息会在后台协程退出前处理完（通道关闭时会排空）。
func Close() {
	// 关闭通道，通知后台协程退出
	if std.logCh != nil {
		close(std.logCh)
	}
	// 等待后台协程处理完所有消息并退出
	std.wg.Wait()

	// 最后刷新并关闭文件（此时后台协程已退出，可直接操作）
	std.mu.Lock()
	defer std.mu.Unlock()
	std.stopFlusher()
	if std.bufw != nil {
		std.bufw.Flush()
		std.bufw = nil
	}
	if std.logFile != nil {
		if err := std.logFile.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close logfile error: %v\n", err)
		}
		std.logFile = nil
	}
}

// ==================== 日志输出函数 ====================

// Info 输出 INFO 级别日志，格式同 fmt.Printf。
//
// 参数 format 为格式串，args 为对应的格式化参数。
// 日志会包含时间戳、协程 ID、文件名行号以及格式化的消息。
// 该调用是非阻塞的，若内部通道已满，日志将被丢弃并在 stderr 输出警告。
func Info(format string, args ...any) {
	Write(LevelInfo, "INFO", true, format, args...)
}

// Error 输出 ERROR 级别日志，格式同 fmt.Printf，并附加调用栈信息（过滤掉框架内部帧）。
//
// 行为同 Info，但级别为 Error。
func Error(format string, args ...any) {
	Write(LevelError, "ERROR", true, format, args...)
}

// Warn 输出 WARN 级别日志，格式同 fmt.Printf。
func Warn(format string, args ...any) {
	Write(LevelWarn, "WARN", true, format, args...)
}

// Debug 输出 DEBUG 级别日志，格式同 fmt.Printf。
func Debug(format string, args ...any) {
	Write(LevelDebug, "DEBUG", true, format, args...)
}

// Write 是底层的日志输出函数，允许自定义级别标签和控制调用位置与正文之间的空格。
//
// 参数：
//   - lv: 日志级别（用于级别过滤）。
//   - levelTag: 日志级别标签（如 "INFO"、"ERROR"），可自定义。
//   - leadingSpace: 若为 true，在调用位置信息与正文之间插入一个空格；否则无缝拼接。
//   - format: fmt.Printf 风格的格式串。
//   - args: 格式参数。
//
// 该函数会被 Info/Error/Warn/Debug 调用，也可直接使用以实现自定义标签或格式。
// 注意：如果 lv 低于当前设置的最低级别，该调用会被忽略（快速返回）。
//
// 并发安全且非阻塞。
func Write(lv Level, levelTag string, leadingSpace bool, format string, args ...any) {
	if lv < Level(std.level.Load()) {
		return
	}

	cfg := std.config.Load().(*config) // 获取当前配置

	msg := fmt.Sprintf(format, args...)
	if leadingSpace {
		msg = " " + msg
	}
	caller := getCaller(3)

	// Error 级别附加调用栈
	stack := ""
	if lv == LevelError {
		stack = getStack()
	}

	t := time.Now().In(cfg.location)
	prefix := t.Format(timestampLayout) + "|" + levelTag
	line := prefix + caller + msg + stack + "\r\n"

	// 非阻塞发送到通道
	select {
	case std.logCh <- line:
	default:
		// 通道已满，丢弃日志并输出到 stderr（避免业务阻塞）
		fmt.Fprintf(os.Stderr, "[LOG DROPPED] %s", line)
	}
}

// ==================== 内部实现 ====================

func getCaller(skip int) string {
	_, file, line, ok := runtime.Caller(skip)
	if !ok {
		return ""
	}
	fileName := filepath.Base(file)
	return fmt.Sprintf("|%d|%s:%d|", goid.Get(), fileName, line)
}

func getStack() string {
	pcs := make([]uintptr, 32)
	n := runtime.Callers(2, pcs) // 跳过 getStack 和 Write
	if n == 0 {
		return ""
	}
	pcs = pcs[:n]

	frames := runtime.CallersFrames(pcs)
	var b strings.Builder
	for {
		frame, more := frames.Next()
		// 跳过 log 包内部和 runtime 内部的帧
		if !strings.Contains(frame.File, "log/log.go") && !strings.HasPrefix(frame.Function, "runtime.") {
			b.WriteString("\n")
			b.WriteString(filepath.Base(frame.File))
			b.WriteString(":")
			b.WriteString(strconv.Itoa(frame.Line))
		}
		if !more {
			break
		}
	}
	if b.Len() == 0 {
		return ""
	}
	return b.String()
}

func resolveTimezone(name string) (*time.Location, error) {
	name = strings.TrimSpace(name)
	if name == "" || strings.EqualFold(name, "Local") {
		return time.Local, nil
	}
	if strings.EqualFold(name, "Asia/Shanghai") {
		return time.FixedZone("Asia/Shanghai", 8*60*60), nil
	}
	location, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("load timezone[%s]: %w", name, err)
	}
	return location, nil
}

// ---------- 后台写入协程 ----------

func (l *Logger) backgroundWriter() {
	defer l.wg.Done()

	for msg := range l.logCh {
		l.mu.Lock()
		cfg := l.config.Load().(*config)

		// 处理配置变更（重新打开文件）
		if l.reopen {
			l.reopen = false
			// 关闭旧文件并重置缓冲区
			if l.bufw != nil {
				l.bufw.Flush()
				l.bufw = nil
			}
			if l.logFile != nil {
				l.logFile.Close()
				l.logFile = nil
				l.logTime = time.Time{}
			}
			// 停止旧定时器，稍后根据新配置重启
			l.stopFlusher()
		}

		// 如果文件未打开，打开它（使用当前时间）
		if l.logFile == nil {
			now := time.Now().In(cfg.location)
			if err := l.rotate(now); err != nil {
				fmt.Fprintf(os.Stderr, "open logfile error: %v\n", err)
				l.mu.Unlock()
				continue
			}
			// 如果配置了刷新间隔，启动定时器
			if cfg.flushInterval > 0 {
				l.startFlusher()
			}
		}

		// 写入文件（经过 buffer）
		if l.bufw != nil {
			if _, err := l.bufw.WriteString(msg); err != nil {
				fmt.Fprintf(os.Stderr, "write buffer error: %v\n", err)
				// 尝试直接写文件
				if l.logFile != nil {
					l.logFile.WriteString(msg)
				}
			}
			// 如果缓冲区满，立即刷新
			if l.bufw.Buffered() >= cfg.bufSize {
				if err := l.bufw.Flush(); err != nil {
					fmt.Fprintf(os.Stderr, "flush buffer error: %v\n", err)
				}
			}
		} else if l.logFile != nil {
			if _, err := l.logFile.WriteString(msg); err != nil {
				fmt.Fprintf(os.Stderr, "write logfile error: %v\n", err)
			}
		}

		// 控制台输出
		if cfg.console {
			fmt.Print(msg)
		}

		l.mu.Unlock()
	}

	// 通道关闭，退出前刷新并关闭文件
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stopFlusher()
	if l.bufw != nil {
		l.bufw.Flush()
		l.bufw = nil
	}
	if l.logFile != nil {
		l.logFile.Close()
		l.logFile = nil
		l.logTime = time.Time{}
	}
}

// ---------- 轮转与清理 ----------

func (l *Logger) rotate(t time.Time) error {
	if !l.needRotate(t) {
		return nil
	}

	cfg := l.config.Load().(*config)

	if err := os.MkdirAll(cfg.dir, 0755); err != nil {
		return fmt.Errorf("create log dir[%s]: %w", cfg.dir, err)
	}

	baseName := fmt.Sprintf("%s_%04d%02d%02d", cfg.prefix, t.Year(), t.Month(), t.Day())
	fileName := filepath.Join(cfg.dir, baseName+".log")

	if err := rotateFileIfNeeded(fileName, cfg.dir, baseName, cfg.maxSize); err != nil {
		return err
	}

	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open logfile[%s]: %w", fileName, err)
	}

	// 关闭旧文件前刷新旧 buffer
	if l.bufw != nil {
		l.bufw.Flush()
	}
	oldFile := l.logFile
	l.logFile = f
	l.logTime = t

	// 重建缓冲（根据当前 bufSize）
	if cfg.bufSize > 0 {
		l.bufw = bufio.NewWriterSize(f, cfg.bufSize)
	} else {
		l.bufw = nil
	}

	if oldFile != nil {
		if err := oldFile.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close old logfile error: %v\n", err)
		}
	}

	l.cleanupOldFiles(t)
	return nil
}

func (l *Logger) needRotate(t time.Time) bool {
	if l.logFile == nil {
		return true
	}
	if l.logTime.Year() != t.Year() ||
		l.logTime.Month() != t.Month() ||
		l.logTime.Day() != t.Day() {
		return true
	}
	stat, err := l.logFile.Stat()
	if err != nil {
		return true
	}
	cfg := l.config.Load().(*config)
	return stat.Size() >= cfg.maxSize
}

func rotateFileIfNeeded(fileName, dir, baseName string, maxSize int64) error {
	stat, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat logfile[%s]: %w", fileName, err)
	}
	if stat.Size() < maxSize {
		return nil
	}

	bak, err := nextBackupName(dir, baseName)
	if err != nil {
		return err
	}
	if err := os.Rename(fileName, bak); err != nil {
		return fmt.Errorf("rename logfile[%s] to [%s]: %w", fileName, bak, err)
	}
	return nil
}

func nextBackupName(dir, baseName string) (string, error) {
	for idx := 1; ; idx++ {
		bak := filepath.Join(dir, fmt.Sprintf("%s_%d.log", baseName, idx))
		_, err := os.Stat(bak)
		if os.IsNotExist(err) {
			return bak, nil
		}
		if err != nil {
			return "", fmt.Errorf("stat logfile backup[%s]: %w", bak, err)
		}
	}
}

// cleanupOldFiles 删除超出限制的备份文件。
// 匹配文件名格式：prefix_YYYYMMDD[_N].log（N 可选，仅用于验证）。
// 根据 maxAge 和 maxBackups 删除旧文件（按文件修改时间排序）。
// 使用正则匹配确保前缀中的下划线不会干扰解析。
func (l *Logger) cleanupOldFiles(now time.Time) {
	cfg := l.config.Load().(*config)
	if cfg.maxBackups == 0 && cfg.maxAge == 0 {
		return // 无限制，无需清理
	}

	entries, err := os.ReadDir(cfg.dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cleanup read dir error: %v\n", err)
		return
	}

	// 构造正则：^prefix_(\d{8})(?:_(\d+))?\.log$
	pattern := `^` + regexp.QuoteMeta(cfg.prefix) + `_(\d{8})(?:_(\d+))?\.log$`
	re := regexp.MustCompile(pattern)

	// ---------- FIXED: 获取当前正在写入的文件路径（用于排除） ----------
	currentFilePath := ""
	if l.logFile != nil {
		currentFilePath = l.logFile.Name()
	}

	var files []os.FileInfo
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !re.MatchString(name) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		// 跳过当前正在写入的文件（若有）
		fullPath := filepath.Join(cfg.dir, name)
		if fullPath == currentFilePath {
			continue
		}
		files = append(files, info)
	}
	// ----------------------------------------------------------------

	if len(files) == 0 {
		return
	}

	// 按修改时间排序（从旧到新）
	sort.Slice(files, func(i, j int) bool {
		return files[i].ModTime().Before(files[j].ModTime())
	})

	// 根据 maxAge 删除过期文件
	if cfg.maxAge > 0 {
		cutoff := now.Add(-cfg.maxAge)
		var keep []os.FileInfo
		for _, f := range files {
			if f.ModTime().Before(cutoff) {
				path := filepath.Join(cfg.dir, f.Name())
				if err := os.Remove(path); err != nil {
					fmt.Fprintf(os.Stderr, "cleanup remove old file %s error: %v\n", path, err)
				}
			} else {
				keep = append(keep, f)
			}
		}
		files = keep
	}

	// 根据 maxBackups 删除多余文件（保留最新的 N 个）
	if cfg.maxBackups > 0 && len(files) > cfg.maxBackups {
		toDelete := len(files) - cfg.maxBackups
		for i := 0; i < toDelete; i++ {
			path := filepath.Join(cfg.dir, files[i].Name())
			if err := os.Remove(path); err != nil {
				fmt.Fprintf(os.Stderr, "cleanup remove excess file %s error: %v\n", path, err)
			}
		}
	}
}

// ---------- 缓冲刷新器 ----------

func (l *Logger) startFlusher() {
	if l.flushTicker != nil {
		l.stopFlusher()
	}
	cfg := l.config.Load().(*config)
	l.flushTicker = time.NewTicker(cfg.flushInterval)
	l.flushDone = make(chan struct{})
	go l.flushLoop()
}

func (l *Logger) stopFlusher() {
	if l.flushTicker != nil {
		l.flushTicker.Stop()
		close(l.flushDone)
		l.flushTicker = nil
		l.flushDone = nil
	}
}

func (l *Logger) flushLoop() {
	for {
		select {
		case <-l.flushTicker.C:
			l.mu.Lock()
			if l.bufw != nil {
				if err := l.bufw.Flush(); err != nil {
					fmt.Fprintf(os.Stderr, "background flush error: %v\n", err)
				}
			}
			l.mu.Unlock()
		case <-l.flushDone:
			return
		}
	}
}

// ---------- 文件名清理 ----------

func sanitizePrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return defaultPrefix
	}

	var b strings.Builder
	lastUnderscore := false
	for _, r := range prefix {
		if isUnsafePrefixRune(r) {
			if !lastUnderscore {
				b.WriteByte('_')
				lastUnderscore = true
			}
			continue
		}
		b.WriteRune(r)
		lastUnderscore = false
	}

	cleaned := strings.ReplaceAll(b.String(), "..", "_")
	cleaned = strings.Trim(cleaned, "._ ")
	if cleaned == "" {
		return defaultPrefix
	}
	return cleaned
}

func isUnsafePrefixRune(r rune) bool {
	switch r {
	case '/', '\\', ':', '*', '?', '"', '<', '>', '|':
		return true
	default:
		return r < 32
	}
}
