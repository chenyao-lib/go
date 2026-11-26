package log

import "io"

// fileBuffer 保存短写（partial write）之后尚未写出的剩余内容。
// 后续写入会优先重试这段剩余内容，因此既不会越过它，也不会重复已写出的字节。
type fileBuffer struct {
	dst   io.Writer
	data  []byte
	limit int
	retry bool
}

func newFileBuffer(dst io.Writer, limit int) *fileBuffer {
	return &fileBuffer{dst: dst, limit: limit}
}

// Buffered 返回当前缓存中尚未写出的字节数。
func (b *fileBuffer) Buffered() int { return len(b.data) }

// WriteString 把 value 追加到缓存；缓存达到 limit 时先落盘。
// 底层出现短写时保留未写出的部分并标记 retry，下次写入优先重试。
// 返回的 n 始终是 value 的长度，只有底层出错时才返回非 nil error。
func (b *fileBuffer) WriteString(value string) (int, error) {
	if len(b.data) != 0 && (b.retry || len(value) > b.limit-len(b.data)) {
		if err := b.Flush(); err != nil {
			return 0, err
		}
	}
	if len(b.data) == 0 && len(value) >= b.limit {
		n, err := io.WriteString(b.dst, value)
		if n < len(value) {
			b.data = append(b.data, value[n:]...)
			b.retry = true
			if err == nil {
				err = io.ErrShortWrite
			}
		}
		return len(value), err
	}
	b.data = append(b.data, value...)
	if len(b.data) >= b.limit {
		return len(value), b.Flush()
	}
	return len(value), nil
}

// Flush 把缓存写入目标 writer；短写时保留剩余内容等待下次重试，
// 全部写完且容量超过 limit 时释放底层数组。
func (b *fileBuffer) Flush() error {
	if len(b.data) == 0 {
		return nil
	}
	n, err := b.dst.Write(b.data)
	if n > 0 {
		if n < len(b.data) {
			copy(b.data, b.data[n:])
		}
		b.data = b.data[:len(b.data)-n]
	}
	b.retry = len(b.data) != 0
	if !b.retry && cap(b.data) > b.limit {
		b.data = nil
	}
	if b.retry && err == nil {
		err = io.ErrShortWrite
	}
	return err
}
