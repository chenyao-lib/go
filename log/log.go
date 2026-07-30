// Package log 提供进程级日志输出、按天/大小滚动文件、控制台输出和运行时级别控制。
//
// # 基本用法
//
// 程序启动时初始化一次，并在退出前关闭文件：
//
//	func main() {
//		log.Init("gateway", "./logs", 100*1024*1024, log.LevelInfo)
//		defer log.Close()
//
//		if err := log.SetTimezone("Asia/Shanghai"); err != nil {
//			panic(err)
//		}
//		log.Info("server started at %s", ":8080")
//	}
//
// Init 的 prefix 是日志文件名前缀，dir 为空时使用 logs，maxSize 小于等于
// 0 时使用 100 MiB。日志文件按日期命名；同一天超过 maxSize 后依次生成带
// _1、_2 等后缀的备份文件。SetConsole 可单独关闭控制台输出，SetLevel 可在
// 运行时调整最低输出级别。
//
// # 格式化与静态检查
//
// Debug、Info、Warn、Error 和 Write 都使用 fmt.Printf 风格的格式串：
//
//	log.Info("user=%s retries=%d", userID, retries)
//
// 格式串应尽量使用字符串常量。go vet 和 gopls 能检查格式符对应的参数数量
// 与类型。Error 除正文外还会附加调用栈。
//
// # 直接写入
//
// 通常应使用四个级别函数。需要自定义级别标签或正文排版时可调用 Write：
//
//	log.Write(log.LevelInfo, "INFO", false, "worker| task=%s", taskID)
//
// leadingSpace 为 true 时，在调用位置与正文之间补一个空格；为 false 时正文
// 紧跟调用位置信息。Write 是公开 API，修改其参数时需要同步更新直接调用方。
//
// 包使用全局单例，所有配置和写入均可并发调用，但应用通常只应在启动阶段调用
// Init、SetTimezone 和 SetConsole。
package log

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/outrigdev/goid"
)

// Level 日志级别
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

const (
	defaultPrefix   = "log"
	defaultDir      = "logs"
	defaultMaxSize  = 100 * 1024 * 1024 // 100MB
	timestampLayout = "2006-01-02 15:04:05.000000000"
)

// Logger 全局日志实例（包内单例）
type Logger struct {
	mu       sync.Mutex
	prefix   string
	level    int32
	dir      string
	maxSize  int64
	console  bool
	location *time.Location

	logFile *os.File
	logTime time.Time
}

var std = &Logger{
	level:    int32(LevelDebug), // 默认输出全部级别
	prefix:   defaultPrefix,
	dir:      defaultDir,
	maxSize:  defaultMaxSize,
	console:  true,
	location: time.Local,
}

// Init 初始化日志配置。prefix 为文件名前缀，dir 为空时使用默认目录，maxSize 小于等于 0 时使用默认大小。
func Init(prefix, dir string, maxSize int64, lv Level) {
	prefix = sanitizePrefix(prefix)
	if dir == "" {
		dir = defaultDir
	}
	if maxSize <= 0 {
		maxSize = defaultMaxSize
	}

	std.mu.Lock()
	defer std.mu.Unlock()

	std.prefix = prefix
	std.dir = dir
	std.maxSize = maxSize
	atomic.StoreInt32(&std.level, int32(lv))

	if std.logFile != nil {
		if err := std.logFile.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close logfile error: %v\n", err)
		}
		std.logFile = nil
		std.logTime = time.Time{}
	}
}

// SetLevel 设置最低输出级别。低于此级别的日志将被忽略。
func SetLevel(lv Level) {
	atomic.StoreInt32(&std.level, int32(lv))
}

// SetConsole 设置是否输出到控制台。
func SetConsole(enable bool) {
	std.mu.Lock()
	defer std.mu.Unlock()
	std.console = enable
}

// SetTimezone 设置日志时区。name 为空或为 Local 时使用服务器当地时区。
// 使用 Asia/Shanghai 可让不同服务器统一显示北京时间。
func SetTimezone(name string) error {
	location, err := resolveTimezone(name)
	if err != nil {
		return err
	}

	std.mu.Lock()
	defer std.mu.Unlock()
	std.location = location
	return nil
}

// Close 关闭日志文件。应在程序退出前调用。
func Close() {
	std.mu.Lock()
	defer std.mu.Unlock()
	if std.logFile != nil {
		if err := std.logFile.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close logfile error: %v\n", err)
		}
		std.logFile = nil
	}
}

// ==================== 对外接口 ====================

func Info(format string, args ...any) {
	Write(LevelInfo, "INFO", true, format, args...)
}

func Error(format string, args ...any) {
	Write(LevelError, "ERROR", true, format, args...)
}

func Warn(format string, args ...any) {
	Write(LevelWarn, "WARN", true, format, args...)
}

func Debug(format string, args ...any) {
	Write(LevelDebug, "DEBUG", true, format, args...)
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

// Write 输出一条日志。
func Write(lv Level, levelTag string, leadingSpace bool, format string, args ...any) {
	if !std.shouldLog(lv) {
		return
	}

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

	std.mu.Lock()
	defer std.mu.Unlock()

	if !std.shouldLog(lv) {
		return
	}

	t := time.Now().In(std.location)
	if err := std.rotate(t); err != nil {
		fmt.Fprintf(os.Stderr, "rotate logfile error: %v\n", err)
	}

	prefix := t.Format(timestampLayout) + "|" + levelTag

	line := prefix + caller + msg + stack + "\r\n"
	if std.logFile != nil {
		if _, err := std.logFile.WriteString(line); err != nil {
			fmt.Fprintf(os.Stderr, "write logfile error: %v\n", err)
			if err := std.logFile.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "close logfile error: %v\n", err)
			}
			std.logFile = nil
			std.logTime = time.Time{}
		}
	}
	if std.console {
		fmt.Print(line)
	}
}

func (l *Logger) shouldLog(lv Level) bool {
	return lv >= Level(atomic.LoadInt32(&l.level))
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

func (l *Logger) rotate(t time.Time) error {
	if !l.needRotate(t) {
		return nil
	}

	if err := os.MkdirAll(l.dir, 0755); err != nil {
		return fmt.Errorf("create log dir[%s]: %w", l.dir, err)
	}

	baseName := fmt.Sprintf("%s_%04d%02d%02d", l.prefix, t.Year(), t.Month(), t.Day())
	fileName := filepath.Join(l.dir, baseName+".log")

	if err := rotateFileIfNeeded(fileName, l.dir, baseName, l.maxSize); err != nil {
		return err
	}

	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open logfile[%s]: %w", fileName, err)
	}

	oldFile := l.logFile
	l.logFile = f
	l.logTime = t

	if oldFile != nil {
		if err := oldFile.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close logfile error: %v\n", err)
		}
	}
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
	return err != nil || stat.Size() >= l.maxSize
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
