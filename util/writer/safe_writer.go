package writer

import (
	"io"
	"sync"
	"unicode/utf8"
)

const defaultBufSize = 4096
const maxConsecutiveEmptyReads = 100

// SafeWriter implements highly concurrent buffering for an io.Writer object.
// In particular, writes will not block while a Flush() call is in progress as
// long as enough buffer space is available.
//
// Note however that writes will still block in a number of cases, e.g. when
// another write, larger than the buffer size, is in progress. Also, concurrent
// Flush() calls (whether explicit or triggered by the buffer filling up) will
// block one another.
type SafeWriter struct {
	mtx  *sync.Mutex
	cond *sync.Cond

	err error
	buf []byte
	n   int
	wr  io.Writer

	// Number of bytes currently being flushed from the start of buf. Only
	// non-zero while a flush is in progress. Always <= n.
	nFlushing int
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
	if size <= 0 {
		size = defaultBufSize
	}
	if size < utf8.UTFMax {
		size = utf8.UTFMax
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

	// Wait for in-progress flush() calls to compete
	for b.err == nil && b.nFlushing != 0 {
		b.cond.Wait()
	}
	// (Potentially) wake one other goroutine waiting on flush()
	b.cond.Signal()

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
	err := b.flush(NON_BLOCKING, EXPECT_FLUSH)
	b.mtx.Unlock()
	return err
}

// Whether flush() should prevent concurrent writes or not.
type flushConcurrency int

// Whether the flush() caller expects a fully empty buffer or only some space.
type flushExpectation int

const (
	// Do not allow concurrent writes while flushing. Needed while large writes
	// (larger than available buffer size) are in progress.
	//
	// In practice, when large writes are in progress the buffer will always be
	// full while the mutex is unlocked inside flush() (i.e. b.n == b.nFlushing
	// == len(buf)) so this is not necessary IF callers are well behaved. But even
	// so it is more efficient to not unnecessarily wake a goroutine wating for
	// free buffer space.
	BLOCKING flushConcurrency = iota
	// Allow concurrent writes while flushing.
	NON_BLOCKING
	// The caller expects any data in the buffer to actually be flushed.
	EXPECT_FLUSH flushExpectation = iota
	// The caller expects some space to become available in the buffer, is not
	// interested in actually flushing any buffered data. Useful for preventing
	// unnecessarily flushing small amounts of data when an in-progress flush
	// already freed up buffer space.
	EXPECT_SPACE
)

func (b *SafeWriter) flush(concurrency flushConcurrency, expect flushExpectation) error {
	// Always (potentially) wake one goroutine waiting on flush()
	defer b.cond.Signal()

	// Wait for in-progress flush() calls to complete
	for b.err == nil && b.nFlushing != 0 {
		b.cond.Wait()
		if expect == EXPECT_SPACE && b.available() > 0 {
			return b.err
		}
	}
	if b.err != nil {
		return b.err
	}
	// Return if caller expectation has been met.
	if (expect == EXPECT_FLUSH && b.n == 0) || (expect == EXPECT_SPACE && b.available() > 0) {
		return nil
	}

	b.nFlushing = b.n

	if concurrency == NON_BLOCKING && b.nFlushing != len(b.buf) {
		// Release the mutex to allow concurrent writes
		b.mtx.Unlock()
	}

	// Actually flush the first nFlush bytes
	n, err := b.wr.Write(b.buf[0:b.nFlushing])
	if n < b.nFlushing && err == nil {
		err = io.ErrShortWrite
	}

	if concurrency == NON_BLOCKING && b.nFlushing != len(b.buf) {
		// Grab back the mutex once the potentially blocking I/O calls are done
		b.mtx.Lock()
	}

	if n > 0 && n < b.n {
		copy(b.buf[0:b.n-n], b.buf[n:b.n])
	}
	b.n -= n
	b.err = err
	b.nFlushing = 0

	return b.err
}

// Flush writes any buffered data to the underlying io.Writer.
func (b *SafeWriter) flush2(concurrency flushConcurrency, expect flushExpectation) error {
	if b.err != nil {
		return b.err
	}
	if b.n == 0 {
		return nil
	}
	n, err := b.wr.Write(b.buf[0:b.n])
	if n < b.n && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		if n > 0 && n < b.n {
			copy(b.buf[0:b.n-n], b.buf[n:b.n])
		}
		b.n -= n
		b.err = err
		return err
	}
	b.n = 0
	return nil
}

// Available returns how many bytes are unused in the buffer.
func (b *SafeWriter) Available() int {
	b.mtx.Lock()
	res := b.available()
	b.mtx.Unlock()
	return res
}
func (b *SafeWriter) available() int { return len(b.buf) - b.n }

// Buffered returns the number of bytes that have been written into the current buffer.
func (b *SafeWriter) Buffered() int {
	b.mtx.Lock()
	res := b.buffered()
	b.mtx.Unlock()
	return res
}
func (b *SafeWriter) buffered() int { return b.n }

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
			//			n = len(p) / (len(p) - len(p))
			n = copy(b.buf[b.n:], p)
			b.n += n
			b.flush(BLOCKING, EXPECT_SPACE)
		}
		nn += n
		p = p[n:]
	}
	if b.err != nil {
		return nn, b.err
	}
	n := copy(b.buf[b.n:], p)
	b.n += n
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
	if b.available() <= 0 && b.flush(NON_BLOCKING, EXPECT_SPACE) != nil {
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
	// Keep flushing until enough space is available
	for b.available() < utf8.UTFMax {
		if b.flush(NON_BLOCKING, EXPECT_SPACE); b.err != nil {
			return 0, b.err
		}
	}
	size = utf8.EncodeRune(b.buf[b.n:], r)
	b.n += size
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
		b.n += n
		nn += n
		s = s[n:]
		b.flush(BLOCKING, EXPECT_SPACE)
	}
	if b.err != nil {
		return nn, b.err
	}
	n := copy(b.buf[b.n:], s)
	b.n += n
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
			if err1 := b.flush(BLOCKING, EXPECT_SPACE); err1 != nil {
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
		b.n += m
		n += int64(m)
		if err != nil {
			break
		}
	}
	if err == io.EOF {
		// If we filled the buffer exactly, flush preemptively.
		if b.available() == 0 {
			err = b.flush(NON_BLOCKING, EXPECT_SPACE)
		} else {
			err = nil
		}
	}
	return n, err
}
