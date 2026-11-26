// Package log 提供进程级日志输出，支持按天/大小滚动文件、控制台输出、运行时级别控制、
// 缓冲写入以及备份文件的自动清理（按数量或保留时间）。
//
// # 特性
//   - 零配置启动：即使不调用 Init，也会使用默认配置（./logs 目录、100MB 滚动、DEBUG 级别）。
//     Init 之前的日志先暂存在内存（同时输出控制台），宽限 1 秒内仍未 Init 才按默认配置落盘，
//     因此不会因为启动期日志而多出一个默认前缀的文件。
//   - 非阻塞写入：日志通过通道交给后台协程。队列满时丢弃新日志，由后台汇总丢弃数量。
//   - 并发安全：所有公开函数均可安全地在多个 goroutine 中同时调用。
//   - 动态配置：可在运行时调整级别、保留策略、缓冲参数等，无需重启。
//   - 优雅关闭：Close 会等待所有已入队日志写入完成，确保数据不丢失。
//   - 最新文件入口：每次打开或滚动日志后，在日志目录维护 <prefix>_latest.log
//     硬链接指向当前文件，可用固定路径查看最新日志（跟踪请用 tail -F）。
//     文件系统不支持硬链接时只告警一次，不影响日志写入；异常退出遗留的
//     <prefix>_latest.log.*.tmp 会在下次滚动时立即清理。
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
// 致命退出请使用 log.Fatal / log.FatalM，而不要直接 os.Exit：它们会先刷新缓冲区再退出，
// 避免丢失崩溃现场尚未落盘的日志。
//
// 即使不调用 Init，日志也会默认写入 ./logs/log_YYYYMMDD.log，级别为 DEBUG；
// Init 之前的日志会先缓冲最多 1 秒（可用环境变量 LOG_INIT_GRACE_MS 覆盖，
// 设为 0 关闭缓冲），以保证"启动期日志 + main 里 Init"只产生一个日志文件。
//
// 该包经过并发安全优化，所有配置字段通过 atomic.Value 存储，消除数据竞争；
// 备份文件清理使用正则匹配，避免前缀含下划线导致解析失效。
package log

import (
	"errors"
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

type M = map[string]any

// Level 定义日志级别，数值越小级别越低。
type Level int

const (
	LevelDebug Level = iota // 调试信息
	LevelInfo               // 常规信息
	LevelWarn               // 警告
	LevelError              // 错误（会附带调用栈）
	LevelFatal              // 致命错误（附带调用栈，通常紧接进程退出）
)

const (
	defaultPrefix     = "log"
	defaultDir        = "logs"
	defaultMaxSize    = 100 * 1024 * 1024 // 100MB
	defaultBufSize    = 64 * 1024         // 64KB
	defaultFlushIntvl = 5 * time.Second
	defaultChanCap    = 10000 // 通道容量，最多缓存 10000 条未处理日志
	timestampLayout   = "2006-01-02 15:04:05.000000000"

	// defaultInitGrace 是 Init 前日志的缓冲宽限时间。
	// 进程启动时各包的 init() 早于 main()，若它们写日志，默认会先落到
	// prefix=log 的文件；main 里再调用 Init("app", ...) 就会切换文件，
	// 同一天出现 log_ 与 app_ 两个文件。缓冲宽限让正常的"main 开头 Init"
	// 在落盘前完成切换，只产生 Init 配置的那一个文件。
	defaultInitGrace = time.Second
	// maxPendingLines 是 Init 前缓冲的最大条数，超出后立刻按默认配置落盘，
	// 防止长时间未 Init 的进程无限占用内存。
	maxPendingLines = 2000
	// initGraceEnv 允许用 LOG_INIT_GRACE_MS 覆盖宽限时间（毫秒）。
	// 设为 0 表示关闭缓冲：Init 前的日志尽快按默认配置落盘（后台异步处理
	// 仍可能与 Init 竞争，若 Init 更早生效则直接写入 Init 配置的文件）。
	initGraceEnv = "LOG_INIT_GRACE_MS"
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
	logFile      *os.File
	logTime      time.Time
	bufw         *fileBuffer
	retired      *retiredLogFile
	flushTicker  *time.Ticker
	flushDone    chan struct{}
	reopen       bool // 标记是否需要重新打开文件（配置变更时置 true）
	latestWarned bool // 最近一次维护 latest 链接失败是否已告警，成功一次后重置

	// Init 之前的日志缓冲，受 mu 保护。
	// initialized 为 false 时日志只进 pendingLines 与控制台，不创建文件；
	// Init 会把暂存内容写入新配置的文件；若宽限内没有 Init（或缓冲溢出），
	// 定时器/写入路径会按默认配置落盘，保证零配置用法仍然可用。
	initialized  bool
	pendingLines []string
	pendingSince time.Time
	pendingTimer *time.Timer
	initGrace    time.Duration

	// 异步通道
	logCh  chan string
	stopCh chan struct{}
	sendMu sync.RWMutex // 将入队与关闭接收串行化
	closed atomic.Bool
	wg     sync.WaitGroup

	dropped     atomic.Uint64
	dropPending chan struct{} // 唤醒后台汇报，最多保留一个通知
}

type retiredLogFile struct {
	file    *os.File
	buffer  *fileBuffer
	retryAt time.Time
}

const retiredLogRetryInterval = time.Second

// std 是包级默认实例，在 init 中初始化并启动后台协程。
var std = &Logger{
	logCh:       make(chan string, defaultChanCap),
	stopCh:      make(chan struct{}),
	dropPending: make(chan struct{}, 1),
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
	std.initGrace = resolveInitGrace()

	std.wg.Add(1)
	go std.backgroundWriter()
}

// resolveInitGrace 解析 LOG_INIT_GRACE_MS，非法值回退默认宽限；
// 0 表示关闭缓冲（Init 前的日志尽快落盘）。
func resolveInitGrace() time.Duration {
	raw := strings.TrimSpace(os.Getenv(initGraceEnv))
	if raw == "" {
		return defaultInitGrace
	}
	ms, err := strconv.Atoi(raw)
	if err != nil || ms < 0 {
		fmt.Fprintf(os.Stderr, "invalid %s=%q, use default %s\n", initGraceEnv, raw, defaultInitGrace)
		return defaultInitGrace
	}
	return time.Duration(ms) * time.Millisecond
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
// 注意：若调用时已有文件打开，会在新文件可写后切换到新配置。
// 切换过程中已入队的日志不会丢失。Init 之前产生的日志会暂存在内存中，并在
// Init 时写入新配置的文件，因此"包 init() 先打日志、main 里再 Init"的程序
// 不会在当天同时留下默认前缀与 Init 前缀两个日志文件。
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

	std.initialized = true
	std.reopen = true
	std.stopPendingTimerLocked()
	now := time.Now().In(newCfg.location)

	// Init 前已有日志：按新配置打开文件并补写暂存
	// 内容。这样启动期日志直接进入 Init 指定的文件，不会先落到默认 prefix 的
	// 文件里，同一天也就不会出现两个日志文件。
	if len(std.pendingLines) > 0 {
		if err := std.rotate(now); err != nil {
			fmt.Fprintf(os.Stderr, "open logfile error: %v\n", err)
			// 保留暂存内容，下一条日志打开文件后会补写，避免丢失启动日志。
		} else {
			std.writePendingLocked()
			if std.flushTicker == nil && newCfg.flushInterval > 0 {
				std.startFlusher()
			}
		}
	}

	// 立即执行一次清理（使用新配置）
	std.cleanupOldFiles(now)
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
// 文件已打开时，调整会保留尚未写入的日志，后续写入使用新的缓冲大小。
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

	// 如果文件已打开，保留未写入的字节并更新缓冲大小和刷新器。
	if std.logFile != nil {
		std.stopFlusher()
		std.bufw.limit = newCfg.bufSize
		std.flushLocked()
		if newCfg.flushInterval > 0 {
			std.startFlusher()
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

// closeOnce 保证 Close 只真正执行一次。
// Fatal/FatalM 会先 Close 再 os.Exit，可能与调用方 defer 的 Close 重叠，
// 而重复 close(stopCh) 会 panic，故用 Once 收敛。
var closeOnce sync.Once

// Close 关闭日志系统，刷新所有缓冲区，并等待后台协程将所有已入队日志写入完成。
//
// 应在程序退出前调用（通常使用 defer）。调用后再写入的日志会被直接丢弃（不会 panic）。
//
// 幂等：重复调用安全（仅首次生效），因此可同时被 defer 与 Fatal/FatalM 调用。
func Close() {
	closeOnce.Do(func() {
		std.sendMu.Lock()
		std.closed.Store(true)
		if std.stopCh != nil {
			close(std.stopCh)
		}
		std.sendMu.Unlock()
		// 等待后台协程排空已入队日志并退出
		std.wg.Wait()

		// 兜底：正常情况下后台协程退出前已刷新关闭；此处对 nil 安全，可重复执行
		std.mu.Lock()
		defer std.mu.Unlock()
		std.stopPendingTimerLocked()
		std.closeFileLocked()
	})
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

// Fatal 输出 FATAL 级别日志，随后刷新缓冲区并以状态码 1 退出进程。格式同 fmt.Printf。
//
// 与 Error 的区别是 Fatal 不会返回：它先 Close（排空异步缓冲区、关闭日志文件）再
// os.Exit(1)。致命退出不要直接调用 os.Exit——那会跳过调用方 defer 的 Close，
// 使缓冲区中尚未落盘的日志（通常正是崩溃现场）丢失。
//
// 日志级别为 LevelFatal（≥LevelError，同样附带调用栈）；SetLevel 到 LevelError 及以下都不会过滤掉它。
// 注意：调用后进程立即退出，不会再有任何日志写入；不适用于可恢复的错误。
func Fatal(format string, args ...any) {
	Write(LevelFatal, "FATAL", true, format, args...)
	Close()
	os.Exit(1)
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
// 并发安全，队列满时直接丢弃，由后台汇总。
func Write(lv Level, levelTag string, leadingSpace bool, format string, args ...any) {
	if lv < Level(std.level.Load()) {
		return
	}

	// 已 Close：直接丢弃，避免往无人消费的通道堆积。
	if std.closed.Load() {
		return
	}

	cfg := std.config.Load().(*config) // 获取当前配置

	msg := fmt.Sprintf(format, args...)
	if leadingSpace {
		msg = " " + msg
	}
	caller := getCaller(3)

	// Error 及以上级别附加调用栈（Fatal 亦需要）
	stack := ""
	if lv >= LevelError {
		stack = getStack()
	}

	t := time.Now().In(cfg.location)
	prefix := t.Format(timestampLayout) + "|" + levelTag
	line := prefix + caller + msg + stack + "\r\n"

	std.sendMu.RLock()
	defer std.sendMu.RUnlock()
	if std.closed.Load() {
		return
	}
	select {
	case std.logCh <- line:
	default:
		std.dropped.Add(1)
		select {
		case std.dropPending <- struct{}{}:
		default:
		}
	}
}

// InfoM 输出 INFO 级别结构化日志，字段（map）在前，消息在后。
func InfoM(m M, msg string, args ...any) {
	formatted := fmt.Sprintf(msg, args...)
	fieldStr := formatMap(m)
	Write(LevelInfo, "INFO", false, "%s", formatted+fieldStr)
}

// ErrorM 输出 ERROR 级别结构化日志，字段在前，消息在后，带调用栈。
func ErrorM(m M, msg string, args ...any) {
	formatted := fmt.Sprintf(msg, args...)
	fieldStr := formatMap(m)
	Write(LevelError, "ERROR", false, "%s", formatted+fieldStr)
}

// FatalM 输出 FATAL 级别结构化日志（字段在前，消息在后），随后刷新缓冲区并退出进程。
//
// 行为同 Fatal，只是附带结构化字段，字段格式与 InfoM/ErrorM 一致。
func FatalM(m M, msg string, args ...any) {
	formatted := fmt.Sprintf(msg, args...)
	fieldStr := formatMap(m)
	Write(LevelFatal, "FATAL", false, "%s", formatted+fieldStr)
	Close()
	os.Exit(1)
}

// WarnM 输出 WARN 级别结构化日志。
func WarnM(m M, msg string, args ...any) {
	formatted := fmt.Sprintf(msg, args...)
	fieldStr := formatMap(m)
	Write(LevelWarn, "WARN", false, "%s", formatted+fieldStr)
}

// DebugM 输出 DEBUG 级别结构化日志。
func DebugM(m M, msg string, args ...any) {
	formatted := fmt.Sprintf(msg, args...)
	fieldStr := formatMap(m)
	Write(LevelDebug, "DEBUG", false, "%s", formatted+fieldStr)
}

// ==================== 内部实现 ====================

// formatMap 将 map 格式化为 " key1=val1 key2=val2"，若为空返回空字符串。
func formatMap(m M) string {
	if len(m) == 0 {
		return ""
	}
	var b strings.Builder
	for k, v := range m {
		b.WriteString(", ")
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(fmt.Sprint(v))
	}
	return b.String()
}

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

	for {
		select {
		case msg := <-l.logCh:
			l.handle(msg)
		case <-l.dropPending:
			l.reportDropped()
		case <-l.stopCh:
			// 收到关闭信号：先把通道中已入队的日志排空，再退出。
			// 注意不要关闭 logCh —— 那会让关闭瞬间仍在 Write 的协程 panic。
			for {
				select {
				case msg := <-l.logCh:
					l.handle(msg)
				default:
					l.reportDropped()
					l.flushAndClose()
					return
				}
			}
		}
	}
}

func (l *Logger) reportDropped() {
	if count := l.dropped.Swap(0); count != 0 {
		cfg := l.config.Load().(*config)
		stamp := time.Now().In(cfg.location).Format(timestampLayout)
		l.handle(fmt.Sprintf("%s|WARN%s [LOG DROPPED] %d messages (log queue full)\r\n", stamp, getCaller(1), count))
	}
}

// handle 处理单条日志：按需轮转/打开文件、写入缓冲区并输出到控制台。
func (l *Logger) handle(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	cfg := l.config.Load().(*config)

	now := time.Now().In(cfg.location)

	// Init 之前：日志只写控制台并暂存内存，不创建文件。宽限内 Init 时由 Init
	// 写入其配置的文件；若一直不 Init，则由定时器/溢出按默认配置落盘。
	if !l.initialized && l.logFile == nil {
		l.bufferBeforeInitLocked(msg, now, cfg)
		return
	}
	if cfg.console {
		defer fmt.Print(msg)
	}

	// 如果文件未打开，打开它（使用当前时间）
	if l.logFile == nil {
		if err := l.rotate(now); err != nil {
			fmt.Fprintf(os.Stderr, "open logfile error: %v\n", err)
			return
		}
		// 如果配置了刷新间隔，启动定时器
		if cfg.flushInterval > 0 {
			l.startFlusher()
		}
	} else if l.needRotate(now) {
		// 运行期也要检查跨天 / 超过 maxSize：needRotate 原本只经 rotate()
		// 调用，而 rotate() 只在 logFile == nil（首次打开 / reopen）时执行，
		// 导致进程跨天不换文件、单文件可远超 maxSize
		// （线上实测 app_20261016.log 涨到 421MB，配置上限 100MB）。
		// 轮转失败时报告错误，并在旧文件仍可用时继续写入。
		if err := l.rotate(now); err != nil {
			fmt.Fprintf(os.Stderr, "rotate logfile error: %v\n", err)
		}
	}

	if l.logFile == nil {
		return
	}
	// 补写 Init 时打开文件失败而遗留的暂存日志
	l.writePendingLocked()
	if len(l.pendingLines) == 0 {
		l.writeLineLocked(msg)
	}
	l.retryRetiredLogLocked(now)
}

func (l *Logger) writeLineLocked(line string) (int, error) {
	n, err := l.bufw.WriteString(line)
	if err != nil {
		fmt.Fprintf(os.Stderr, "write logfile %q: %v\n", l.logFile.Name(), err)
	}
	return n, err
}

// bufferBeforeInitLocked 在 Init 尚未调用时暂存日志，并在宽限到期或缓冲溢出时
// 按默认配置落盘。调用方必须持有 mu。
func (l *Logger) bufferBeforeInitLocked(msg string, now time.Time, cfg *config) {
	l.pendingLines = append(l.pendingLines, msg)
	if cfg.console {
		fmt.Print(msg)
	}
	if l.pendingSince.IsZero() {
		l.pendingSince = now
	}

	// 关闭缓冲（initGrace<=0）、缓冲溢出或已超过宽限：按默认配置落盘。
	if l.initGrace <= 0 || len(l.pendingLines) >= maxPendingLines || now.Sub(l.pendingSince) >= l.initGrace {
		l.flushPendingToFileLocked(now)
		return
	}
	if l.pendingTimer == nil {
		delay := l.initGrace - now.Sub(l.pendingSince)
		if delay <= 0 {
			delay = time.Millisecond
		}
		l.pendingTimer = time.AfterFunc(delay, l.onPendingGraceTimeout)
	}
}

// onPendingGraceTimeout 是宽限到期后的兜底：仍未被 Init 时按默认配置落盘。
func (l *Logger) onPendingGraceTimeout() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.pendingTimer = nil
	if l.initialized || l.logFile != nil || len(l.pendingLines) == 0 {
		return
	}
	cfg := l.config.Load().(*config)
	l.flushPendingToFileLocked(time.Now().In(cfg.location))
}

// flushPendingToFileLocked 用当前配置（未 Init 时为默认 prefix=log）打开文件
// 并补写暂存日志。调用方必须持有 mu 且此时 logFile 为 nil。
func (l *Logger) flushPendingToFileLocked(now time.Time) {
	l.stopPendingTimerLocked()
	l.pendingSince = time.Time{}

	cfg := l.config.Load().(*config)
	if err := l.rotate(now); err != nil {
		// 打不开文件时丢弃暂存内容（控制台已输出过），避免无限占用内存。
		fmt.Fprintf(os.Stderr, "open logfile error: %v\n", err)
		l.pendingLines = nil
		return
	}
	l.writePendingLocked()
	if !l.closed.Load() && l.flushTicker == nil && cfg.flushInterval > 0 {
		l.startFlusher()
	}
}

// writePendingLocked 把暂存日志写入当前已打开的文件，并立即刷新一次，
// 保证启动期日志尽快落盘。没有暂存内容时是空操作。
func (l *Logger) writePendingLocked() {
	if len(l.pendingLines) == 0 {
		return
	}
	for len(l.pendingLines) != 0 {
		line := l.pendingLines[0]
		n, err := l.writeLineLocked(line)
		if n == len(line) {
			l.pendingLines[0] = ""
			l.pendingLines = l.pendingLines[1:]
		}
		if err != nil {
			return
		}
	}
	l.pendingLines = nil
	l.flushLocked()
}

// stopPendingTimerLocked 停止宽限兜底定时器。
func (l *Logger) stopPendingTimerLocked() {
	if l.pendingTimer != nil {
		l.pendingTimer.Stop()
		l.pendingTimer = nil
	}
}

// closeFileLocked 停止刷新器并关闭当前文件，调用方必须持有 mu。
func (l *Logger) closeFileLocked() {
	l.stopFlusher()
	l.closeLogFileLocked()
	l.closeRetiredLogLocked()
}

func (l *Logger) flushLocked() error {
	if l.bufw == nil {
		return nil
	}
	if err := l.bufw.Flush(); err != nil {
		name := "<unknown>"
		if l.logFile != nil {
			name = l.logFile.Name()
		}
		fmt.Fprintf(os.Stderr, "flush logfile %q: %v\n", name, err)
		return err
	}
	return nil
}

// closeLogFileLocked closes the file without stopping the periodic flusher during rotation.
func (l *Logger) closeLogFileLocked() {
	if err := l.flushLocked(); err != nil {
		l.flushLocked()
	}
	l.bufw = nil
	if l.logFile != nil {
		closeLogFile(l.logFile)
		l.logFile = nil
		l.logTime = time.Time{}
	}
}

func closeLogFile(file *os.File) {
	if err := file.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close logfile %q: %v\n", file.Name(), err)
	}
}

func (l *Logger) retryRetiredLogLocked(now time.Time) {
	old := l.retired
	if old == nil || now.Before(old.retryAt) {
		return
	}
	if err := old.buffer.Flush(); err != nil && old.buffer.Buffered() != 0 {
		old.retryAt = now.Add(retiredLogRetryInterval)
		return
	}
	l.closeRetiredLogLocked()
}

func (l *Logger) closeRetiredLogLocked() {
	old := l.retired
	if old == nil {
		return
	}
	if err := old.buffer.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "flush retired logfile %q: %v (%d unwritten bytes)\n", old.file.Name(), err, old.buffer.Buffered())
	}
	closeLogFile(old.file)
	l.retired = nil
}

// flushAndClose 退出前刷新缓冲区并关闭文件。
func (l *Logger) flushAndClose() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.stopPendingTimerLocked()
	// Init 打开文件失败时也可能保留了启动日志，关闭前再尝试一次。
	if len(l.pendingLines) > 0 {
		if l.logFile == nil {
			cfg := l.config.Load().(*config)
			l.flushPendingToFileLocked(time.Now().In(cfg.location))
		} else {
			l.writePendingLocked()
		}
	}
	if l.logFile != nil && len(l.pendingLines) > 0 {
		l.writePendingLocked()
	}

	l.closeFileLocked()
}

// ---------- 轮转与清理 ----------

func (l *Logger) rotate(t time.Time) error {
	if !l.needRotate(t) {
		return nil
	}
	cfg := l.config.Load().(*config)
	baseName := fmt.Sprintf("%s_%04d%02d%02d", cfg.prefix, t.Year(), t.Month(), t.Day())
	fileName := filepath.Join(cfg.dir, baseName+".log")
	sameFile := sameLogFile(l.logFile, fileName)
	flushErr := l.flushLocked()
	if flushErr != nil && (!l.reopen || sameFile) {
		return fmt.Errorf("flush before rotating: %w", flushErr)
	}

	if err := os.MkdirAll(cfg.dir, 0755); err != nil {
		return fmt.Errorf("create log dir[%s]: %w", cfg.dir, err)
	}

	var fallbackPath string
	oldTime := l.logTime
	if sameFile {
		// Windows requires closing the active handle before renaming that file.
		fallbackPath = l.logFile.Name()
		l.closeLogFileLocked()
	}

	// 超过 maxSize 时把当前文件转成 _N 备份。改名失败不阻断日志写入：
	// 继续往原文件追加，总比丢日志好。
	backupPath, err := rotateFileIfNeeded(fileName, cfg.dir, baseName, cfg.maxSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "rotate logfile error: %v\n", err)
	}
	if fallbackPath != "" && backupPath != "" {
		fallbackPath = backupPath
	}

	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		openErr := fmt.Errorf("open logfile[%s]: %w", fileName, err)
		if fallbackPath != "" {
			previous, reopenErr := os.OpenFile(fallbackPath, os.O_WRONLY|os.O_APPEND, 0)
			if reopenErr != nil {
				return errors.Join(openErr, fmt.Errorf("reopen previous logfile[%s]: %w", fallbackPath, reopenErr))
			}
			l.setLogFileLocked(previous, oldTime, cfg.bufSize)
		}
		return openErr
	}
	if flushErr != nil && l.bufw.Buffered() != 0 {
		// A path change can recover logging while the old sink is unhealthy.
		// Keep one old file so its unwritten suffix can still be retried in place.
		l.closeRetiredLogLocked()
		l.retired = &retiredLogFile{file: l.logFile, buffer: l.bufw, retryAt: t.Add(retiredLogRetryInterval)}
	} else {
		l.closeLogFileLocked()
	}
	l.setLogFileLocked(f, t, cfg.bufSize)
	l.reopen = false

	// 维护 <prefix>_latest.log 硬链接，固定指向当前文件。软链接在 Windows 需要
	// 建链特权，硬链接同目录、同卷即可，普通账户可用；不支持硬链接的文件系统
	// 只告警，不影响日志写入。
	l.relinkLatestLocked(cfg, fileName)
	l.sweepLatestTmpLocked(cfg)

	l.cleanupOldFiles(t)
	return nil
}

func sameLogFile(file *os.File, path string) bool {
	if file == nil {
		return false
	}
	current, currentErr := file.Stat()
	target, targetErr := os.Stat(path)
	if currentErr == nil && targetErr == nil {
		return os.SameFile(current, target)
	}
	return filepath.Clean(file.Name()) == filepath.Clean(path)
}

func (l *Logger) setLogFileLocked(f *os.File, t time.Time, bufSize int) {
	l.logFile = f
	l.logTime = t
	l.bufw = newFileBuffer(f, bufSize)
}

// latestLinkPath 返回固定指向当前日志文件的硬链接路径。
func latestLinkPath(cfg *config) string {
	return filepath.Join(cfg.dir, cfg.prefix+"_latest.log")
}

// latestTmpPath 返回本次替换使用的唯一临时链接名。
// 不用固定名 .tmp：进程异常退出遗留在 Link 与 Rename 之间的固定名会让后续
// os.Link 永远 EEXIST，临时文件再也无法自愈。唯一名让每次替换互不影响，
// 遗留文件由 sweepLatestTmpLocked 按时间清理。
func latestTmpPath(linkPath string) string {
	return fmt.Sprintf("%s.%d-%d.tmp", linkPath, os.Getpid(), time.Now().UnixNano())
}

// relinkLatestLocked 把 <prefix>_latest.log 重建为指向 fileName 的硬链接。
// 先建唯一临时名再改名替换，替换失败时保留旧链接；失败只告警一次，避免频繁滚动时刷屏。
// 调用方必须持有 mu。
func (l *Logger) relinkLatestLocked(cfg *config, fileName string) {
	linkPath := latestLinkPath(cfg)
	tmpPath := latestTmpPath(linkPath)

	if err := os.Link(fileName, tmpPath); err != nil {
		l.warnLatestLinkLocked("create latest log link", err)
		return
	}
	if err := os.Rename(tmpPath, linkPath); err != nil {
		// 改名失败时立即删掉本次临时链接；删除也失败则交给 sweepLatestTmpLocked。
		_ = os.Remove(tmpPath)
		l.warnLatestLinkLocked("replace latest log link", err)
		return
	}
	l.latestWarned = false
}

// sweepLatestTmpLocked 清理异常退出遗留的 latest 链接临时文件。
// 兼容旧版本的固定名 <prefix>_latest.log.tmp 和新版本的
// <prefix>_latest.log.<pid>-<纳秒>.tmp。
//
// 不能按 mtime 判断新旧：遗留的 .tmp 是指向当前日志的硬链接，与日志共享
// inode，日志每写一行它的 mtime 就变成"刚刚"，按时间过滤永远清不掉。
// 而本进程创建到改名的窗口只有微秒级，且全程持有 mu，扫描时不会看到自己
// 的临时文件，因此这里无条件删除所有匹配项。调用方必须持有 mu。
func (l *Logger) sweepLatestTmpLocked(cfg *config) {
	entries, err := os.ReadDir(cfg.dir)
	if err != nil {
		return
	}
	prefix := cfg.prefix + "_latest.log"
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".tmp") {
			continue
		}
		if err := os.Remove(filepath.Join(cfg.dir, name)); err != nil {
			fmt.Fprintf(os.Stderr, "remove stale latest link tmp error: %v\n", err)
		}
	}
}

func (l *Logger) warnLatestLinkLocked(action string, err error) {
	if l.latestWarned {
		return
	}
	l.latestWarned = true
	fmt.Fprintf(os.Stderr, "%s error: %v (将保留旧链接，下次滚动时重试)\n", action, err)
}

func (l *Logger) needRotate(t time.Time) bool {
	if l.logFile == nil || l.reopen {
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

	// 缓冲区里可能还压着最多一个 bufSize 的字节没有落盘，只看磁盘大小会让
	// 判定整整滞后一个缓冲区——maxSize 小于 bufSize（如 64KB）时永远触发不了。
	size := stat.Size()
	if l.bufw != nil {
		size += int64(l.bufw.Buffered())
	}

	return size >= cfg.maxSize
}

func rotateFileIfNeeded(fileName, dir, baseName string, maxSize int64) (string, error) {
	stat, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("stat logfile[%s]: %w", fileName, err)
	}
	if !stat.Mode().IsRegular() || stat.Size() < maxSize {
		return "", nil
	}

	bak, err := nextBackupName(dir, baseName)
	if err != nil {
		return "", err
	}
	if err := os.Rename(fileName, bak); err != nil {
		return "", fmt.Errorf("rename logfile[%s] to [%s]: %w", fileName, bak, err)
	}
	return bak, nil
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
		// Init 可能在首次写日志前调用，此时目录尚不存在，无需清理也无需报错。
		if !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "cleanup read dir error: %v\n", err)
		}
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
	ticker := time.NewTicker(cfg.flushInterval)
	done := make(chan struct{})
	l.flushTicker = ticker
	l.flushDone = done
	// 把 ticker/done 作为参数传给协程：stopFlusher 会把字段置 nil，
	// 若协程每轮 select 都读 l.flushTicker.C，就会在停止后空指针 panic。
	go l.flushLoop(ticker, done)
}

func (l *Logger) stopFlusher() {
	if l.flushTicker != nil {
		l.flushTicker.Stop()
		close(l.flushDone)
		l.flushTicker = nil
		l.flushDone = nil
	}
}

func (l *Logger) flushLoop(ticker *time.Ticker, done <-chan struct{}) {
	for {
		select {
		case <-ticker.C:
			l.mu.Lock()
			l.flushLocked()
			l.retryRetiredLogLocked(time.Now())
			l.mu.Unlock()
		case <-done:
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
