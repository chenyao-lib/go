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
	dir         = "logs"
	maxFileSize = 100 * 1024 * 1024 // 100MB
)

// Logger 全局日志实例（包内单例）
type Logger struct {
	mu       sync.Mutex
	fileName string
	level    Level

	logFile *os.File
	logTime time.Time
}

var std = &Logger{
	level: LevelDebug, // 默认输出全部级别
}

// Init 初始化日志文件名（不含扩展名）。需在 main 开头调用一次。
func Init(fileName string) {
	std.fileName = fileName
}

// SetLevel 设置最低输出级别。低于此级别的日志将被忽略。
// 默认 LevelDebug，生产环境可设置为 LevelInfo 或 LevelWarn。
func SetLevel(lv Level) {
	std.level = lv
}

// Close 关闭日志文件。应在程序退出前调用。
func Close() {
	std.mu.Lock()
	defer std.mu.Unlock()
	if std.logFile != nil {
		std.logFile.Close()
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
	if lv < std.level {
		return
	}

	msg := fmt.Sprintf(format, args...)
	caller := getCaller(3)

	// Error 级别附加调用栈
	stack := ""
	if lv == LevelError {
		stack = getStack()
	}

	std.mu.Lock()
	defer std.mu.Unlock()

	t := time.Now()
	std.rotate(t)

	prefix := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d|%s",
		t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), levelTag)

	line := prefix + caller + msg + stack + "\r\n"
	if std.logFile != nil {
		std.logFile.WriteString(line)
	}
	fmt.Print(line)
}

func (l *Logger) rotate(t time.Time) {
	if !l.needRotate(t) {
		return
	}

	oldFile := l.logFile

	if !pathExists(dir) {
		os.Mkdir(dir, 0755)
	}

	base := fmt.Sprintf("%s/%s_%d%02d%02d", dir, l.fileName, t.Year(), t.Month(), t.Day())
	fileName := base + ".log"

	if pathExists(fileName) {
		if stat, _ := os.Stat(fileName); stat != nil && stat.Size() >= maxFileSize {
			for idx := 1; ; idx++ {
				bak := fmt.Sprintf("%s_%d.log", base, idx)
				if !pathExists(bak) {
					os.Rename(fileName, bak)
					break
				}
			}
		}
	}

	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open logfile[%s] error: %v\n", fileName, err)
		return
	}

	l.logFile = f
	l.logTime = t

	if oldFile != nil {
		oldFile.Close()
	}
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
	stat, _ := l.logFile.Stat()
	return stat != nil && stat.Size() >= maxFileSize
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
