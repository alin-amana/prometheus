package writer

import (
	//"fmt"
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
	// Protects all fields
	mtx *sync.Mutex

	err error
	buf []byte
	n   int
	wr  io.Writer

	// Fields below are only used by flush() (and marginally by Reset()) to
	// provide correct serialization of writes.
	//
	// When a chunked write calls flush() and finds a concurrent flush already
	// in progress, it sets chunkedWriteInFlight and calls noChunkedWrite.Wait().
	// When a non-chunked write calls flush() and finds that either (1) a
	// concurrent flush already in progress; or (2) chunkedWriteInFlight; it calls
	// notFlushing.Wait().
	// When a flush() call terminates it calls noChunkedWrite.Signal() iff
	// chunkedWriteInFlight; or nonBlocking.Signal() otherwise.

	// Number of bytes currently being flushed from the start of buf. Only
	// non-zero while a flush is in progress. Always <= n.
	nFlush int
	// Condition variable for all goroutines waiting to flush()
	notFlushing *sync.Cond
	// Condition variable that the only chunked writer is waiting on to flush()
	chunkedWriter *sync.Cond

	// Whether if a chunked write is in flight
	chunkedWriteInFlight bool
	// Condition variable for all goroutines waiting on a chunked write
	noChunkedWrite *sync.Cond
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
		mtx:            m,
		buf:            make([]byte, size),
		wr:             w,
		notFlushing:    sync.NewCond(m),
		chunkedWriter:  sync.NewCond(m),
		noChunkedWrite: sync.NewCond(m),
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
	for b.err == nil && b.nFlush != 0 {
		b.notFlushing.Wait()
	}
	// (Potentially) wake one other goroutine waiting on flush()
	b.notFlushing.Signal()

	b.err = nil
	b.n = 0
	b.wr = w
	b.nFlush = 0
	b.mtx.Unlock()
}

// Flush writes any buffered data to the underlying io.Writer. As long as the
// buffer has enough available space, writes can proceed concurrently.
func (b *SafeWriter) Flush() error {
	b.mtx.Lock()
	b.waitUntilNoChunkedWrite()

	err := b.flush(0, false)

	b.mtx.Unlock()
	return err
}

func (b *SafeWriter) flush(need int, iAmChunked bool) error {
	callerNeedMet := func() bool {
		return b.n == 0 || (need > 0 && b.available() >= need)
	}

	// Waits for any in-progress flush() to complete, returns true iff mtx was
	// released in the process (i.e. if any Wait() call was made).
	waitToFlush := func() bool {
		waited := false
		if iAmChunked {
			for b.nFlush != 0 {
				b.chunkedWriter.Wait()
				waited = true
			}
		} else {
			for b.nFlush != 0 || b.chunkedWriteInFlight {
				b.notFlushing.Wait()
				waited = true
			}
		}
		return waited
	}

	// Loop as long as no error and caller need is not met
	for b.err == nil && !callerNeedMet() {
		if waitToFlush() {
			continue
		}

		b.nFlush = b.n

		mtxReleased := false
		if !iAmChunked && b.nFlush != len(b.buf) {
			// Release the mutex to allow concurrent writes
			b.mtx.Unlock()
			mtxReleased = true
		}

		// Actually flush the first nFlush bytes
		n, err := b.wr.Write(b.buf[0:b.nFlush])
		if n < b.nFlush && err == nil {
			err = io.ErrShortWrite
		}

		if mtxReleased {
			// Grab back the mutex once the potentially blocking I/O call is done
			b.mtx.Lock()
		}

		if n > 0 && n < b.n {
			copy(b.buf[0:b.n-n], b.buf[n:b.n])
		}
		b.n -= n
		b.err = err
		b.nFlush = 0

		if mtxReleased && b.chunkedWriteInFlight {
			// We are not the chunked writer but one exists: wake it and block until
			// it is done (else our caller will enter into a race condition with it).
			b.chunkedWriter.Signal()
			b.waitUntilNoChunkedWrite()
		}

		if need == 0 {
			// Flush() call, no actual buffer space required, all done
			break
		}
	}

	// Always (potentially) wake a goroutine waiting to flush()
	b.notFlushing.Signal()
	return b.err
}

// Flush writes any buffered data to the underlying io.Writer.
func (b *SafeWriter) flush2(need int) error {
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

func (b *SafeWriter) beginChunkedWrite() int {
	b.chunkedWriteInFlight = true
	return 0
}

func (b *SafeWriter) endChunkedWrite(ignored int) {
	b.chunkedWriteInFlight = false
	// Wake all goroutines waiting to write
	b.noChunkedWrite.Signal()
}

func (b *SafeWriter) waitUntilNoChunkedWrite() {
	// Wait for any in-progress chunked write to complete
	for b.chunkedWriteInFlight {
		b.noChunkedWrite.Wait()
	}
	b.noChunkedWrite.Signal()

	//	if b.chunkedWriteInFlight {
	//		panic("Holy cow!")
	//	}
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
	b.waitUntilNoChunkedWrite()

	for len(p) > b.available() && b.err == nil {
		// Starting chunked write, begin blocking concurrent writes
		if !b.chunkedWriteInFlight {
			defer b.endChunkedWrite(b.beginChunkedWrite())
		}

		var n int
		if b.buffered() == 0 {
			// Large write, empty buffer.
			// Write directly from p to avoid copy.
			n, b.err = b.wr.Write(p)
		} else {
			n = copy(b.buf[b.n:], p)
			b.n += n
			b.flush(1, true)
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
	b.waitUntilNoChunkedWrite()

	if b.err != nil {
		return b.err
	}

	if b.available() <= 0 && b.flush(1, false) != nil {
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

	var encoded [4]byte
	size = utf8.EncodeRune(encoded[:], r)
	return b.Write(encoded[:size])

	//	b.mtx.Lock()
	//	defer b.mtx.Unlock()
	//	b.waitUntilNoChunkedWrite()
	//
	//	if b.err != nil {
	//		return 0, b.err
	//	}
	//
	//	if b.available() < utf8.UTFMax {
	//		//defer b.endChunkedWrite(b.beginChunkedWrite())
	//		// Actually flush the buffer if there are less than 4 bytes available
	//		if b.flush(utf8.UTFMax, false); b.err != nil {
	//			return 0, b.err
	//		}
	//	}
	//	size = utf8.EncodeRune(b.buf[b.n:], r)
	//	b.n += size
	//	return size, nil
}

// WriteString writes a string.
// It returns the number of bytes written.
// If the count is less than len(s), it also returns an error explaining
// why the write is short.
func (b *SafeWriter) WriteString(s string) (int, error) {
	nn := 0
	b.mtx.Lock()
	defer b.mtx.Unlock()
	b.waitUntilNoChunkedWrite()

	if b.err != nil {
		return 0, b.err
	}

	for len(s) > b.available() {
		n := copy(b.buf[b.n:], s)
		b.n += n
		nn += n
		s = s[n:]
		if !b.chunkedWriteInFlight {
			// Starting chunked write, begin blocking concurrent writes
			defer b.endChunkedWrite(b.beginChunkedWrite())
		}
		b.flush(1, true)
		if b.err != nil {
			return nn, b.err
		}
	}
	//	fmt.Printf("s = \"%s\" available = %d\n", s, b.available())
	n := copy(b.buf[b.n:], s)
	b.n += n
	nn += n
	return nn, nil
}

// ReadFrom implements io.ReaderFrom.
func (b *SafeWriter) ReadFrom(r io.Reader) (n int64, err error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	b.waitUntilNoChunkedWrite()

	if b.buffered() == 0 {
		if w, ok := b.wr.(io.ReaderFrom); ok {
			return w.ReadFrom(r)
		}
	}
	// Starting chunked write, begin blocking concurrent writes
	//	if !b.chunkedWriteInFlight {
	//		defer b.endChunkedWrite(b.beginChunkedWrite())
	//	}
	var m int
	for {
		if b.available() == 0 {
			if err1 := b.flush(1, b.chunkedWriteInFlight); err1 != nil {
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
		// Starting chunked write, begin blocking concurrent writes
		if !b.chunkedWriteInFlight {
			defer b.endChunkedWrite(b.beginChunkedWrite())
		}
	}
	if err == io.EOF {
		// If we filled the buffer exactly, flush preemptively.
		if b.available() == 0 {
			err = b.flush(1, b.chunkedWriteInFlight)
		} else {
			err = nil
		}
	}
	return n, err
}
