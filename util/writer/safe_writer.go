package writer

import (
	"io"
	"sync"
	"sync/atomic"
	"unicode/utf8"
)

const (
	defaultBufSize           = 4096
	maxConsecutiveEmptyReads = 100
)

// SafeWriter implements highly concurrent buffering for an io.Writer object.
//
// In particular, writes will not block while a Flush() call is in progress as
// long as more space is available in the buffer than the amount of data being
// written. Note however that writes will still block in a number of cases,
// e.g. when a Flush() vall is in progress and there is not enough available
// space in the buffer or when another write larger than the buffer size is in
// progress.
type SafeWriter struct {
	mtx  *sync.Mutex
	cond *sync.Cond

	err error
	buf []byte
	n   int64
	wr  io.Writer

	// Number of bytes being flushed from the start of buf. Only non-zero when a
	// flush is in progress, always <= n.
	nFlushing int64
}

// NewSafeWriterSize returns a new SafeWriter whose buffer has at least the
// specified size. If the argument io.Writer is already a SafeWriter with large
// enough size, it returns the underlying SafeWriter.
func NewSafeWriterSize(w io.Writer, size int) *SafeWriter {
	// Is it already a SafeWriter?
	b, ok := w.(*SafeWriter)
	if ok && len(b.buf) >= size {
		return b
	}
	if size < utf8.UTFMax {
		size = defaultBufSize
	}
	m := new(sync.Mutex)
	return &SafeWriter{
		mtx:  m,
		cond: sync.NewCond(m),
		buf:  make([]byte, size),
		wr:   w,
	}
}

// NewSafeWriter returns a new SafeWriter whose buffer has the default size.
func NewSafeWriter(w io.Writer) *SafeWriter {
	return NewSafeWriterSize(w, defaultBufSize)
}

// Reset discards any unflushed buffered data, clears any error, and
// resets b to write its output to w.
func (b *SafeWriter) Reset(w io.Writer) {
	b.mtx.Lock()
	b.err = nil
	b.n = 0
	b.wr = w
	b.nFlushing = 0
	b.mtx.Unlock()
}

// Flush writes any buffered data to the underlying io.Writer. As long as the
// buffer has enough available space, writes can proceed concurrently.
func (b *SafeWriter) Flush() error {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	return b.flush(len(b.buf))
}

func (b *SafeWriter) flush(needed int) error {
	if b.err != nil {
		return b.err
	}
	if b.n == 0 {
		return nil
	}
	// Wait until enough space is available
	for needed > b.available() {
		if b.nFlushing != 0 {
			// Flush in progress, wait and see
			b.cond.Wait()
			continue
		}

		b.nFlushing = b.n

		// Release the mutex to allow concurrent writes
		// TODO Avoid live locking?
		b.mtx.Unlock()

		// Actually flush the first nFlush bytes
		n, err := b.wr.Write(b.buf[0:b.nFlushing])
		n64 := int64(n)
		if n64 < b.nFlushing && err == nil {
			err = io.ErrShortWrite
		}

		// Grab back the mutex and update the state
		b.mtx.Lock()

		if n64 > 0 && n64 < b.n {
			copy(b.buf[0:b.n-n64], b.buf[n64:b.n])
		}
		b.n -= n64
		b.err = err

		// Done flushing.
		b.nFlushing = 0
		// Awake one goroutine waiting for flushing to complete.
		b.cond.Signal()
	}
	return b.err
}

// Available returns how many bytes are unused in the buffer.
func (b *SafeWriter) Available() int { return len(b.buf) - int(atomic.LoadInt64(&b.n)) }

func (b *SafeWriter) available() int { return len(b.buf) - int(b.n) }

// Buffered returns the number of bytes that have been written into the current buffer.
func (b *SafeWriter) Buffered() int { return int(atomic.LoadInt64(&b.n)) }

func (b *SafeWriter) buffered() int { return int(b.n) }

// Write writes the contents of p into the buffer.
// It returns the number of bytes written.
// If nn < len(p), it also returns an error explaining
// why the write is short.
func (b *SafeWriter) Write(p []byte) (nn int, err error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	for len(p) > b.available() && b.err == nil {
		var n int
		if b.buffered() == 0 {
			// Large write, empty buffer.
			// Write directly from p to avoid copy.
			n, b.err = b.wr.Write(p)
		} else {
			n = copy(b.buf[b.n:], p)
			b.n += int64(n)
			b.flush(1)
		}
		nn += n
		p = p[n:]
	}
	if b.err != nil {
		return nn, b.err
	}
	n := copy(b.buf[b.n:], p)
	b.n += int64(n)
	nn += n
	return nn, nil
}

// WriteByte writes a single byte.
func (b *SafeWriter) WriteByte(c byte) error {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	if b.err != nil {
		return b.err
	}
	if b.available() <= 0 && b.flush(1) != nil {
		return b.err
	}
	b.buf[b.n] = c
	b.n++
	return nil
}

// WriteRune writes a single Unicode code point, returning
// the number of bytes written and any error.
func (b *SafeWriter) WriteRune(r rune) (size int, err error) {
	if r < utf8.RuneSelf {
		err = b.WriteByte(byte(r))
		if err != nil {
			return 0, err
		}
		return 1, nil
	}
	b.mtx.Lock()
	defer b.mtx.Unlock()
	if b.err != nil {
		return 0, b.err
	}
	if b.available() < utf8.UTFMax {
		if b.flush(utf8.UTFMax); b.err != nil {
			return 0, b.err
		}
	}
	size = utf8.EncodeRune(b.buf[b.n:], r)
	b.n += int64(size)
	return size, nil
}

// WriteString writes a string.
// It returns the number of bytes written.
// If the count is less than len(s), it also returns an error explaining
// why the write is short.
func (b *SafeWriter) WriteString(s string) (int, error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	nn := 0
	for len(s) > b.available() && b.err == nil {
		n := copy(b.buf[b.n:], s)
		b.n += int64(n)
		nn += n
		s = s[n:]
		b.flush(1)
	}
	if b.err != nil {
		return nn, b.err
	}
	n := copy(b.buf[b.n:], s)
	b.n += int64(n)
	nn += n
	return nn, nil
}

// ReadFrom implements io.ReaderFrom.
func (b *SafeWriter) ReadFrom(r io.Reader) (n int64, err error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	if b.buffered() == 0 {
		if w, ok := b.wr.(io.ReaderFrom); ok {
			return w.ReadFrom(r)
		}
	}
	var m int
	for {
		if b.available() == 0 {
			if err1 := b.flush(1); err1 != nil {
				return n, err1
			}
		}
		nr := 0
		for nr < maxConsecutiveEmptyReads {
			m, err = r.Read(b.buf[b.n:])
			if m != 0 || err != nil {
				break
			}
			nr++
		}
		if nr == maxConsecutiveEmptyReads {
			return n, io.ErrNoProgress
		}
		b.n += int64(m)
		n += int64(m)
		if err != nil {
			break
		}
	}
	if err == io.EOF {
		// If we filled the buffer exactly, flush preemptively.
		if b.available() == 0 {
			err = b.flush(1)
		} else {
			err = nil
		}
	}
	return n, err
}
