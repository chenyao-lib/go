package srv

import "time"

// inboundLimiter 由单条连接的读协程独占。两个令牌桶分别按消息数和字节数计费。
type inboundLimiter struct {
	messageRate float64
	byteRate    float64
	byteBurst   float64
	messages    float64
	bytes       float64
	last        time.Time
}

func newInboundLimiter(messageRate int, byteRate, readLimit int64) inboundLimiter {
	byteBurst := float64(max(byteRate, readLimit))
	return inboundLimiter{
		messageRate: float64(messageRate),
		byteRate:    float64(byteRate),
		byteBurst:   byteBurst,
		messages:    float64(messageRate),
		bytes:       byteBurst,
	}
}

func (l *inboundLimiter) allow(now time.Time, size int) bool {
	if !l.last.IsZero() {
		elapsed := now.Sub(l.last).Seconds()
		l.messages = min(l.messageRate, l.messages+elapsed*l.messageRate)
		l.bytes = min(l.byteBurst, l.bytes+elapsed*l.byteRate)
	}
	l.last = now

	cost := float64(size)
	if l.messages < 1 || l.bytes < cost {
		return false
	}
	l.messages--
	l.bytes -= cost
	return true
}
