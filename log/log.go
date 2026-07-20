package log

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
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
	defaultPrefix  = "log"
	defaultDir     = "logs"
	defaultMaxSize = 100 * 1024 * 1024 // 100MB
)

// Logger 全局日志实例（包内单例）
type Logger struct {
	mu      sync.Mutex
	prefix  string
	level   Level
	dir     string
	maxSize int64

	logFile *os.File
	logTime time.Time
}

var std = &Logger{
	level:   LevelDebug, // 默认输出全部级别
	prefix:  defaultPrefix,
	dir:     defaultDir,
	maxSize: defaultMaxSize,
}

// Init 初始化日志配置。prefix 为文件名前缀，dir 为空时使用默认目录，maxSize 小于等于 0 时使用默认大小。
func Init(prefix, dir string, maxSize int64, lv Level) {
	if prefix == "" {
		prefix = defaultPrefix
	}
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
	std.level = lv

	if std.logFile != nil {
		if err := std.logFile.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close logfile error: %v\n", err)
		}
		std.logFile = nil
		std.logTime = time.Time{}
	}
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
	Write(LevelInfo, "INFO", " "+format, args...)
}

func Error(format string, args ...any) {
	Write(LevelError, "ERROR", " "+format, args...)
}

func Warn(format string, args ...any) {
	Write(LevelWarn, "WARN", " "+format, args...)
}

func Debug(format string, args ...any) {
	Write(LevelDebug, "DEBUG", " "+format, args...)
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
	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)
	if n == 0 {
		return ""
	}
	lines := strings.SplitN(string(buf[:n]), "\n", 3)
	// 跳过 runtime.Stack 和 getStack 自身两帧，从第3行开始
	if len(lines) >= 3 {
		return "\n" + strings.Join(lines[2:], "\n")
	}
	return string(buf[:n])
}

// Write 输出一条日志。
func Write(lv Level, levelTag string, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	caller := getCaller(3)

	// Error 级别附加调用栈
	stack := ""
	if lv == LevelError {
		stack = getStack()
	}

	std.mu.Lock()
	defer std.mu.Unlock()

	if lv < std.level {
		return
	}

	t := time.Now()
	if err := std.rotate(t); err != nil {
		fmt.Fprintf(os.Stderr, "rotate logfile error: %v\n", err)
	}

	prefix := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d|%s",
		t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), levelTag)

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
	fmt.Print(line)
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
