package writer

import (
	"io"
	"sync"
	"unicode/utf8"
)

const defaultBufSize = 4096
const maxConsecutiveEmptyReads = 100

// Whether concurrent writes should be blocked or not. Blocking is needed while
// chunked writes (larger than the initially available buffer size) are in
// progress.
type writerMode int

// Whether the flush() caller expects a fully empty buffer or only some space.
type flushExpectation int

const (
	// Allow concurrent writes.
	NON_BLOCKING writerMode = iota
	// Block concurrent writes.
	BLOCKING

	// The caller expects all data currently in the buffer to actually be flushed.
	EXPECT_FLUSH flushExpectation = iota
	// The caller expects some space to become available in the buffer, it is not
	// interested in actually flushing all buffered data. Useful for preventing
	// unnecessarily flushing small amounts of data when an in-progress flush
	// will free up buffer space.
	EXPECT_SPACE
)

// SafeWriter implements highly concurrent buffering for an io.Writer object.
// In particular, writes will not block while a Flush() call is in progress as
// long as enough buffer space is available.
//
// Note however that writes will still block in a number of cases, e.g. when
// another write, larger than the buffer size, is in progress. Also, concurrent
// Flush() calls (whether explicit or triggered by the buffer filling up) will
// block one another.
type SafeWriter struct {
	// Protects all fields
	mtx *sync.Mutex

	err error
	buf []byte
	n   int
	wr  io.Writer

	// Fields below are only used by flush() (and marginally by Reset()) to
	// provide correct serialization of writes.
	//
	// When a blocking write calls flush() and finds a concurrent flush already
	// in progress, it sets mode = BLOCKING and calls blocking.Wait().
	// When a non-blocking write calls flush() and finds that either (1) a
	// concurrent flush already in progress; or (2) mode == BLOCKING; it calls
	// non_blocking.Wait().
	// When a flush() call terminates it calls blocking.Signal() iff
	// mode == BLOCKING; or nonBlocking.Signal() otherwise.

	// Number of bytes currently being flushed from the start of buf. Only
	// non-zero while a flush is in progress. Always <= n.
	nFlushing int
	// true if a blocking write is waiting, false otherwise
	mode writerMode
	// Condition variable for the one blocking write in progress
	blocking *sync.Cond
	// Condition variable for all non-blocking writes
	nonBlocking *sync.Cond
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
		mtx:         m,
		buf:         make([]byte, size),
		wr:          w,
		mode:        NON_BLOCKING,
		blocking:    sync.NewCond(m),
		nonBlocking: sync.NewCond(m),
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

	// Wait for in-progress flush() call(s) to complete
	for b.err == nil && b.nFlushing != 0 {
		b.nonBlocking.Wait()
	}
	// (Potentially) wake one other goroutine waiting on flush()
	b.nonBlocking.Signal()

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
	b.waitIfBlockingWriteInFlight()

	err := b.flush(EXPECT_FLUSH)

	b.mtx.Unlock()
	return err
}

func (b *SafeWriter) flush(expect flushExpectation) error {
	// Always (potentially) wake one goroutine waiting on flush()
	defer b.nonBlocking.Signal()

	if b.err != nil {
		return b.err
	}

	// Wait for any in-progress flush() to complete
	for b.nFlushing != 0 {
		b.nonBlocking.Wait()
	}

	// Return if caller expectation has been met.
	if (expect == EXPECT_FLUSH && b.n == 0) || (expect == EXPECT_SPACE && b.available() > 0) {
		return nil
	}

	b.nFlushing = b.n

	mtxReleased := false
	if b.mode == NON_BLOCKING && b.nFlushing != len(b.buf) {
		// Release the mutex to allow concurrent writes
		b.mtx.Unlock()
		mtxReleased = true
	}

	// Actually flush the first nFlush bytes
	n, err := b.wr.Write(b.buf[0:b.nFlushing])
	if n < b.nFlushing && err == nil {
		err = io.ErrShortWrite
	}

	if mtxReleased {
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
func (b *SafeWriter) flush2(expect flushExpectation) error {
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

func (b *SafeWriter) beginBlockingWrite() int {
	b.mode = BLOCKING
	return 0
}

func (b *SafeWriter) endBlockingWrite(ignored int) {
	b.mode = NON_BLOCKING
	// Wake all goroutines waiting to write
	b.blocking.Signal()
}

func (b *SafeWriter) waitIfBlockingWriteInFlight() {
	// Wait for any in-progress flush() to complete
	for b.mode == BLOCKING {
		b.blocking.Wait()
	}
	b.blocking.Signal()
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
	b.waitIfBlockingWriteInFlight()

	for len(p) > b.available() && b.err == nil {
		var n int
		if b.buffered() == 0 {
			// Large write, empty buffer.
			// Write directly from p to avoid copy.
			n, b.err = b.wr.Write(p)
		} else {
			// Start blocking concurrent writes as soon as we do a partial write
			defer b.endBlockingWrite(b.beginBlockingWrite())

			n = copy(b.buf[b.n:], p)
			b.n += n
			b.flush(EXPECT_FLUSH)
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
	b.waitIfBlockingWriteInFlight()

	if b.available() <= 0 && b.flush(EXPECT_SPACE) != nil {
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
	b.waitIfBlockingWriteInFlight()

	// Keep flushing until enough space is available
	for b.available() < utf8.UTFMax {
		if b.flush(EXPECT_SPACE); b.err != nil {
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
	nn := 0
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if b.err != nil {
		return 0, b.err
	}
	b.waitIfBlockingWriteInFlight()

	for len(s) > b.available() {
		n := copy(b.buf[b.n:], s)
		b.n += n
		nn += n
		s = s[n:]
		if len(s) > 0 && b.mode == NON_BLOCKING {
			// Start blocking concurrent writes as soon as we started a partial write
			defer b.endBlockingWrite(b.beginBlockingWrite())
		}
		b.flush(EXPECT_SPACE)
		if b.err != nil {
			return nn, b.err
		}
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
	b.waitIfBlockingWriteInFlight()

	if b.buffered() == 0 {
		if w, ok := b.wr.(io.ReaderFrom); ok {
			return w.ReadFrom(r)
		}
	}
	var m int
	for {
		if b.available() == 0 {
			if err1 := b.flush(EXPECT_SPACE); err1 != nil {
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
		// Start blocking concurrent writes as soon as we put some data into b.buf
		if b.mode == NON_BLOCKING {
			defer b.endBlockingWrite(b.beginBlockingWrite())
		}
	}
	if err == io.EOF {
		// If we filled the buffer exactly, flush preemptively.
		if b.available() == 0 {
			err = b.flush(EXPECT_SPACE)
		} else {
			err = nil
		}
	}
	return n, err
}
