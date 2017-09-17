package writer

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"unicode/utf8"
)

// An io.Writer and io.ReaderFrom that only accepts a predetermined set of
// inputs, each having a different first byte.
type presetWriter struct {
	presets [][]byte
	// How many times each preset was encountered
	count []int
	// Which preset starts with the given byte
	presetFor []int
	// Curently matched preset
	cur int
	// Position within matched preset
	pos int
	// Bytes written
	written int
	// Last written bytes
	last [64]byte
}

func (w *presetWriter) context(current []byte, pos int) string {
	var before, after []byte
	if pos < len(w.last) {
		lenBefore := len(w.last)
		if lenBefore > w.written {
			lenBefore = w.written
		}
		before = make([]byte, lenBefore)
		copy(before, w.last[len(w.last)-lenBefore+pos:])
		copy(before[lenBefore-pos:], current)
	} else {
		before = current[pos-len(w.last) : pos]
	}
	lenAfter := len(current) - pos
	if lenAfter > len(w.last) {
		lenAfter = len(w.last)
	}
	after = current[pos : pos+lenAfter]
	return fmt.Sprintf("before %q, after %q", before, after)
}

func NewPresetWriter(presets ...[]byte) *presetWriter {
	if len(presets) == 0 {
		panic("No presets")
	}
	var presetFor [256]int
	for i := 0; i < 256; i++ {
		presetFor[i] = -1
	}
	for i := 0; i < len(presets); i++ {
		firstByte := presets[i][0]
		if presetFor[firstByte] != -1 {
			panic(fmt.Sprintf("2 presets with same first byte: %q and %q", presets[i], presets[presetFor[firstByte]]))
		}
		presetFor[firstByte] = i
	}

	return &presetWriter{
		presets:   presets,
		count:     make([]int, len(presets)),
		presetFor: presetFor[:],
		cur:       -1,
	}
}

func (w *presetWriter) Write(p []byte) (int, error) {
	for i := 0; i < len(p); i++ {
		if w.cur == -1 || w.pos >= len(w.presets[w.cur]) {
			w.cur = w.presetFor[p[i]]
			w.pos = 0
			if w.cur == -1 {
				return i, fmt.Errorf("unexpected byte written @%d: %q, %s", w.written, p[i], w.context(p, i))
			}
		} else if p[i] != w.presets[w.cur][w.pos] {
			return i, fmt.Errorf("unexpected byte written @%d(%d): %q, expected %q, %s", w.written, i, p[i], w.presets[w.cur][w.pos], w.context(p, i))
		}
		w.pos++
		w.written++
		if w.pos >= len(w.presets[w.cur]) {
			w.count[w.cur]++
		}
	}
	if len(p) < len(w.last) {
		copy(w.last[0:], w.last[len(p):])
		copy(w.last[len(w.last)-len(p):], p)
	} else {
		copy(w.last[:], p[len(p)-len(w.last):])
	}
	return len(p), nil
}

func (w *presetWriter) ReadFrom(r io.Reader) (n int64, err error) {
	var buf [1024]byte
	for {
		m, e := r.Read(buf[0:len(buf)])
		m2, e2 := w.Write(buf[:m])
		n += int64(m2)
		if e2 != nil {
			return n, e2
		}
		if m2 != m {
			panic("Expected either all bytes consumed or an error")
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return n, e
		}
	}
	return n, nil // err is EOF, so return nil explicitly
}

var bufsizes = []int{
	0, 7, 16, 23, 32, 46, 64, 93, 128, 1024,
}

func TestConcurrentWrites(t *testing.T) {
	var data [1024]byte
	for i := 0; i < len(data); i++ {
		data[i] = byte('a' + i%('z'-'a'))
	}
	stringdata := string(data[:])
	runedata := '\U0002070E'
	bdata := byte('X')

	for i := 0; i < len(bufsizes); i++ {
		for j := 0; j < len(bufsizes); j++ {
			nwrite := bufsizes[i]
			bs := bufsizes[j]
			sdata := stringdata[:nwrite]

			// Write nwrite bytes, an nwrite length string, an nwrite length reader,
			// a 4 byte rune or one byte, using buffer size bs from nr goroutines.
			// Check that the right count of each makes it out.
			nr := 1000
			var w *presetWriter
			var expectedCount []int
			if nwrite == 0 {
				w = NewPresetWriter([]byte(string(runedata)), []byte{bdata})
				expectedCount = []int{nr / 5, nr / 5}
			} else {
				w = NewPresetWriter(data[:nwrite], []byte(string(runedata)), []byte{bdata})
				expectedCount = []int{3 * nr / 5, nr / 5, nr / 5}
			}
			buf := NewWriterSize(w, bs)
			context := fmt.Sprintf("nwrite=%d bufsize=%d", nwrite, bs)

			var wg sync.WaitGroup
			wg.Add(nr)
			for k := 0; k < nr; k++ {
				go func(index int) {
					defer wg.Done()

					switch index % 5 {
					case 0:
						n, e1 := buf.Write(data[0:nwrite])
						if e1 != nil || n != nwrite {
							t.Errorf("%s: buf.Write = %d, %v", context, n, e1)
							return
						}
					case 1:
						n, e1 := buf.WriteString(sdata)
						if e1 != nil || n != nwrite {
							t.Errorf("%s: buf.WriteString = %d, %v", context, n, e1)
							return
						}
					case 2:
						r := bytes.NewReader(data[0:nwrite])
						n, e1 := buf.ReadFrom(r)
						if e1 != nil || n != int64(nwrite) {
							t.Errorf("%s: buf.ReadFrom = %d, %v", context, n, e1)
							return
						}
					case 3:
						n, e1 := buf.WriteRune(runedata)
						if e1 != nil || n != 4 {
							t.Errorf("%s: buf.WriteRune = %d, %v", context, n, e1)
							return
						}
					case 4:
						e1 := buf.WriteByte(bdata)
						if e1 != nil {
							t.Errorf("%s: buf.WriteByte = %v", context, e1)
							return
						}
					}

					// Liberally sprinkle some Flush() calls
					if index%7 == 0 {
						if e := buf.Flush(); e != nil {
							t.Errorf("%s: buf.Flush = %v", context, e)
						}
					}
				}(k)
			}
			wg.Wait()
			buf.Flush()

			for k := 0; k < len(expectedCount); k++ {
				if expectedCount[k] != w.count[k] {
					t.Errorf("%s: Expected %d, got %d writes of %q", context, expectedCount[k], w.count[k], w.presets[k])
				}
			}
		}
	}
}

func TestConcurrentWrites(t *testing.T) {
	var data [1024]byte
	runedata := '\U0002070E'
	bdata := byte('X')

	for i := 0; i < len(data); i++ {
		data[i] = byte('a' + i%('z'-'a'))
	}
	stringdata := string(data[:])

	var runebytes [4]byte
	utf8.EncodeRune(runebytes[:], runedata)

	w := new(bytes.Buffer)
	for i := 0; i < len(bufsizes); i++ {
		for j := 0; j < len(bufsizes); j++ {
			nwrite := bufsizes[i]
			bs := bufsizes[j]
			sdata := stringdata[:nwrite]

			// Write nwrite bytes using buffer size bs from each of 100 goroutines.
			// Check that the right amount makes it out and that the data is correct.

			w.Reset()
			buf := NewWriterSize(w, bs)
			context := fmt.Sprintf("nwrite=%d bufsize=%d", nwrite, bs)

			var wg sync.WaitGroup
			nr := 1000
			wg.Add(nr)
			for k := 0; k < nr; k++ {
				go func(index int) {
					defer wg.Done()

					switch index % 5 {
					case 0:
						n, e1 := buf.Write(data[0:nwrite])
						if e1 != nil || n != nwrite {
							t.Errorf("%s: buf.Write = %d, %v", context, n, e1)
							return
						}
					case 1:
						n, e1 := buf.WriteString(sdata)
						if e1 != nil || n != nwrite {
							t.Errorf("%s: buf.WriteString = %d, %v", context, n, e1)
							return
						}
					case 2:
						r := bytes.NewReader(data[0:nwrite])
						n, e1 := buf.ReadFrom(r)
						if e1 != nil || n != int64(nwrite) {
							t.Errorf("%s: buf.ReadFrom = %d, %v", context, n, e1)
							return
						}
					case 3:
						n, e1 := buf.WriteRune(runedata)
						if e1 != nil || n != 4 {
							t.Errorf("%s: buf.WriteRune = %d, %v", context, n, e1)
							return
						}
					case 4:
						e1 := buf.WriteByte(bdata)
						if e1 != nil {
							t.Errorf("%s: buf.WriteByte = %v", context, e1)
							return
						}
					}

					// Liberally sprinkle some Flush() calls
					if index%7 == 0 {
						if e := buf.Flush(); e != nil {
							t.Errorf("%s: buf.Flush = %v", context, e)
						}
					}
				}(k)
			}
			wg.Wait()
			buf.Flush()

			written := w.Bytes()
			expected := nr / 5 * (3*nwrite + 4 + 1)
			if len(written) != expected {
				t.Errorf("%s: %d bytes expected, %d written", context, expected, len(written))
			}
			for l := 0; l < len(written); l++ {
				var expData []byte
				switch written[l] {
				case data[0]:
					expData = data[:nwrite]
				case runebytes[0]:
					expData = runebytes[:]
				case bdata:
					expData = []byte{bdata}
				default:
					t.Errorf("%s: unexpected byte written @%d: %q", context, l, written[l])
					t.Errorf("  want %d * %q, %d * %#U, %d * '%c'", 3*nr/5, data[0:nwrite], nr/5, runedata, nr/5, bdata)
					t.Fatalf("  have=%s", written)
				}
				for ll := 0; ll < len(expData); ll++ {
					if written[l] != expData[ll] {
						t.Errorf("%s: wrong bytes written @%d: %q expected: %q", context, l, written[l], expData[ll])
						t.Errorf("  want %d * %q, %d * %#U, %d * '%c'", 3*nr/5, data[0:nwrite], nr/5, runedata, nr/5, bdata)
						t.Fatalf("  have=%s", written)
					}
					l++
				}
				l--
			}
		}
	}
}

func TestConcurrentReset(t *testing.T) {
	var data [1024]byte
	for i := 0; i < len(data); i++ {
		data[i] = byte('a' + i%('z'-'a'))
	}
	stringdata := string(data[:])
	runedata := '\U0002070E'
	bdata := byte('X')

	for i := 0; i < len(bufsizes); i++ {
		for j := 0; j < len(bufsizes); j++ {
			nwrite := bufsizes[i]
			bs := bufsizes[j]
			if bs == 0 {
				continue
			}
			sdata := stringdata[:nwrite]

			// Write nwrite bytes, an nwrite length string, an nwrite length reader,
			// a 4 byte rune or one byte, using buffer size bs from nr goroutines.

			nr := 1000
			var w1, w2 *presetWriter
			if nwrite == 0 {
				w1 = NewPresetWriter([]byte(string(runedata)), []byte{bdata})
				w2 = NewPresetWriter([]byte(string(runedata)), []byte{bdata})
			} else {
				w1 = NewPresetWriter(data[:nwrite], []byte(string(runedata)), []byte{bdata})
				w2 = NewPresetWriter(data[:nwrite], []byte(string(runedata)), []byte{bdata})
			}
			buf := NewWriterSize(w1, bs)
			context := fmt.Sprintf("nwrite=%d bufsize=%d", nwrite, bs)

			var wg, wgReset sync.WaitGroup
			nr := 10
			wg.Add(nr)
			wgReset.add(nr/2 - 1)
			for k := 0; k < nr; k++ {
				go func(index int) {
					defer wg.Done()
					if index > nr/2 {
						wgReset.Wait()
					}

					switch index % 5 {
					case 0:
						buf.Write(data[0:nwrite])
					case 1:
						buf.WriteString(sdata)
					case 2:
						r := bytes.NewReader(data[0:nwrite])
						buf.ReadFrom(r)
					case 3:
						buf.WriteRune(runedata)
					case 4:
						buf.WriteByte(bdata)
					}

					// Liberally sprinkle some Flush() calls
					if index%7 == 0 {
						buf.Flush()
					}

					// And reset the buffer once, halfway through
					if index == nr/2 {
						buf.Reset(w2)
					} else if index < nr/2 {
						wgReset.Done()
					}
				}(k)
			}
			wg.Wait()
			buf.Flush()

			maxExpected := nr / 5 * (3*nwrite + 4 + 1)
			minExpected := maxExpected - bs
			if minExpected < 0 {
				minExpected = 0
			}
			actual := w1.written + w2.written
			if actual < minExpected || actual > maxExpected {
				t.Errorf("%s: %d - %d bytes expected, %d written", context, minExpected, maxExpected, actual)
			}
		}
	}
}

// A callbackBuffer is like bytes.Buffer, but Write() will invoke the given
// callback then reset it. It counts the number of times Write is called on it.
type callbackBuffer struct {
	buf      bytes.Buffer
	n        int
	callback func()
}

func (w *callbackBuffer) Write(p []byte) (int, error) {
	w.n++
	if w.callback != nil {
		w.callback()
		w.callback = nil
	}
	return w.buf.Write(p)
}

func TestFlushDoesNotBlockWrite(t *testing.T) {
	var data [10]byte
	for i := 0; i < len(data); i++ {
		data[i] = byte('0' + i)
	}

	w := new(callbackBuffer)
	buf := NewWriterSize(w, 1024)

	nr := 100
	var isFlushing, writesDone sync.WaitGroup
	isFlushing.Add(1)
	writesDone.Add(nr)
	for i := 0; i < nr; i++ {
		go func(index int) {
			isFlushing.Wait()
			buf.Write(data[:])
			writesDone.Done()
		}(i)
	}

	w.callback = func() {
		// Flush() is in progress, unblock writer goroutines
		isFlushing.Done()
		// And wait for all writes to complete before allowing the flush to continue
		writesDone.Wait()
	}

	checkNWrites := func(n int) {
		if w.n != n {
			t.Errorf("Want %d writes, got %d", n, w.n)
		}
	}

	// Write some data to buf, check that no write made it through to w
	buf.Write(data[:])
	checkNWrites(0)

	// Explicitly flush, check that it made it through
	buf.Flush()
	checkNWrites(1)

	// Write goroutines have all completed by now, flush and check it went through
	buf.Flush()
	checkNWrites(2)

	// No other writes, Flush() should be a noop
	buf.Flush()
	checkNWrites(2)

	written := w.buf.Bytes()
	if len(written) != len(data)*(nr+1) {
		t.Errorf("%d bytes written", len(written))
	}
	for l := 0; l < len(written); l++ {
		if written[l] != data[l%len(data)] {
			t.Errorf("wrong bytes written")
			t.Errorf("want %d * %q", nr+1, data)
			t.Fatalf("have=%q", written)
		}
	}
}

func TestWriter(t *testing.T) {
	var data [8192]byte

	for i := 0; i < len(data); i++ {
		data[i] = byte(' ' + i%('~'-' '))
	}
	w := new(bytes.Buffer)
	for i := 0; i < len(bufsizes); i++ {
		for j := 0; j < len(bufsizes); j++ {
			nwrite := bufsizes[i]
			bs := bufsizes[j]

			// Write nwrite bytes using buffer size bs.
			// Check that the right amount makes it out
			// and that the data is correct.

			w.Reset()
			buf := NewWriterSize(w, bs)
			context := fmt.Sprintf("nwrite=%d bufsize=%d", nwrite, bs)
			n, e1 := buf.Write(data[0:nwrite])
			if e1 != nil || n != nwrite {
				t.Errorf("%s: buf.Write = %d, %v", context, n, e1)
				continue
			}
			if e := buf.Flush(); e != nil {
				t.Errorf("%s: buf.Flush = %v", context, e)
			}

			written := w.Bytes()
			if len(written) != nwrite {
				t.Errorf("%s: %d bytes written", context, len(written))
			}
			for l := 0; l < len(written); l++ {
				if written[l] != data[l] {
					t.Errorf("wrong bytes written")
					t.Errorf("want=%q", data[0:len(written)])
					t.Errorf("have=%q", written)
				}
			}
		}
	}
}

// Check that write errors are returned properly.

type errorWriterTest struct {
	n, m   int
	err    error
	expect error
}

func (w errorWriterTest) Write(p []byte) (int, error) {
	return len(p) * w.n / w.m, w.err
}

var errorWriterTests = []errorWriterTest{
	{0, 1, nil, io.ErrShortWrite},
	{1, 2, nil, io.ErrShortWrite},
	{1, 1, nil, nil},
	{0, 1, io.ErrClosedPipe, io.ErrClosedPipe},
	{1, 2, io.ErrClosedPipe, io.ErrClosedPipe},
	{1, 1, io.ErrClosedPipe, io.ErrClosedPipe},
}

func TestWriteErrors(t *testing.T) {
	for _, w := range errorWriterTests {
		buf := NewWriter(w)
		_, e := buf.Write([]byte("hello world"))
		if e != nil {
			t.Errorf("Write hello to %v: %v", w, e)
			continue
		}
		// Two flushes, to verify the error is sticky.
		for i := 0; i < 2; i++ {
			e = buf.Flush()
			if e != w.expect {
				t.Errorf("Flush %d/2 %v: got %v, wanted %v", i+1, w, e, w.expect)
			}
		}
	}
}

func TestNewWriterSizeIdempotent(t *testing.T) {
	const BufSize = 1000
	b := NewWriterSize(new(bytes.Buffer), BufSize)
	// Does it recognize itself?
	b1 := NewWriterSize(b, BufSize)
	if b1 != b {
		t.Error("NewWriterSize did not detect underlying Writer")
	}
	// Does it wrap if existing buffer is too small?
	b2 := NewWriterSize(b, 2*BufSize)
	if b2 == b {
		t.Error("NewWriterSize did not enlarge buffer")
	}
}

func TestWriteString(t *testing.T) {
	const BufSize = 8
	buf := new(bytes.Buffer)
	b := NewWriterSize(buf, BufSize)
	b.WriteString("0")                         // easy
	b.WriteString("123456")                    // still easy
	b.WriteString("7890")                      // easy after flush
	b.WriteString("abcdefghijklmnopqrstuvwxy") // hard
	b.WriteString("z")
	if err := b.Flush(); err != nil {
		t.Error("WriteString", err)
	}
	s := "01234567890abcdefghijklmnopqrstuvwxyz"
	if string(buf.Bytes()) != s {
		t.Errorf("WriteString wants %q gets %q", s, string(buf.Bytes()))
	}
}

func createTestInput(n int) []byte {
	input := make([]byte, n)
	for i := range input {
		// 101 and 251 are arbitrary prime numbers.
		// The idea is to create an input sequence
		// which doesn't repeat too frequently.
		input[i] = byte(i % 251)
		if i%101 == 0 {
			input[i] ^= byte(i / 101)
		}
	}
	return input
}

func TestWriterReadFrom(t *testing.T) {
	ws := []func(io.Writer) io.Writer{
		func(w io.Writer) io.Writer { return onlyWriter{w} },
		func(w io.Writer) io.Writer { return w },
	}

	rs := []func(io.Reader) io.Reader{
		iotest.DataErrReader,
		func(r io.Reader) io.Reader { return r },
	}

	for ri, rfunc := range rs {
		for wi, wfunc := range ws {
			input := createTestInput(8192)
			b := new(bytes.Buffer)
			w := NewWriter(wfunc(b))
			r := rfunc(bytes.NewReader(input))
			if n, err := w.ReadFrom(r); err != nil || n != int64(len(input)) {
				t.Errorf("ws[%d],rs[%d]: w.ReadFrom(r) = %d, %v, want %d, nil", wi, ri, n, err, len(input))
				continue
			}
			if err := w.Flush(); err != nil {
				t.Errorf("Flush returned %v", err)
				continue
			}
			if got, want := b.String(), string(input); got != want {
				t.Errorf("ws[%d], rs[%d]:\ngot  %q\nwant %q\n", wi, ri, got, want)
			}
		}
	}
}

type errorReaderFromTest struct {
	rn, wn     int
	rerr, werr error
	expected   error
}

func (r errorReaderFromTest) Read(p []byte) (int, error) {
	return len(p) * r.rn, r.rerr
}

func (w errorReaderFromTest) Write(p []byte) (int, error) {
	return len(p) * w.wn, w.werr
}

var errorReaderFromTests = []errorReaderFromTest{
	{0, 1, io.EOF, nil, nil},
	{1, 1, io.EOF, nil, nil},
	{0, 1, io.ErrClosedPipe, nil, io.ErrClosedPipe},
	{0, 0, io.ErrClosedPipe, io.ErrShortWrite, io.ErrClosedPipe},
	{1, 0, nil, io.ErrShortWrite, io.ErrShortWrite},
}

func TestWriterReadFromErrors(t *testing.T) {
	for i, rw := range errorReaderFromTests {
		w := NewWriter(rw)
		if _, err := w.ReadFrom(rw); err != rw.expected {
			t.Errorf("w.ReadFrom(errorReaderFromTests[%d]) = _, %v, want _,%v", i, err, rw.expected)
		}
	}
}

// TestWriterReadFromCounts tests that using io.Copy to copy into a
// bufio.Writer does not prematurely flush the buffer. For example, when
// buffering writes to a network socket, excessive network writes should be
// avoided.
func TestWriterReadFromCounts(t *testing.T) {
	var w0 writeCountingDiscard
	b0 := NewWriterSize(&w0, 1234)
	b0.WriteString(strings.Repeat("x", 1000))
	if w0 != 0 {
		t.Fatalf("write 1000 'x's: got %d writes, want 0", w0)
	}
	b0.WriteString(strings.Repeat("x", 200))
	if w0 != 0 {
		t.Fatalf("write 1200 'x's: got %d writes, want 0", w0)
	}
	io.Copy(b0, onlyReader{strings.NewReader(strings.Repeat("x", 30))})
	if w0 != 0 {
		t.Fatalf("write 1230 'x's: got %d writes, want 0", w0)
	}
	io.Copy(b0, onlyReader{strings.NewReader(strings.Repeat("x", 9))})
	if w0 != 1 {
		t.Fatalf("write 1239 'x's: got %d writes, want 1", w0)
	}

	var w1 writeCountingDiscard
	b1 := NewWriterSize(&w1, 1234)
	b1.WriteString(strings.Repeat("x", 1200))
	b1.Flush()
	if w1 != 1 {
		t.Fatalf("flush 1200 'x's: got %d writes, want 1", w1)
	}
	b1.WriteString(strings.Repeat("x", 89))
	if w1 != 1 {
		t.Fatalf("write 1200 + 89 'x's: got %d writes, want 1", w1)
	}
	io.Copy(b1, onlyReader{strings.NewReader(strings.Repeat("x", 700))})
	if w1 != 1 {
		t.Fatalf("write 1200 + 789 'x's: got %d writes, want 1", w1)
	}
	io.Copy(b1, onlyReader{strings.NewReader(strings.Repeat("x", 600))})
	if w1 != 2 {
		t.Fatalf("write 1200 + 1389 'x's: got %d writes, want 2", w1)
	}
	b1.Flush()
	if w1 != 3 {
		t.Fatalf("flush 1200 + 1389 'x's: got %d writes, want 3", w1)
	}
}

// A writeCountingDiscard is like ioutil.Discard and counts the number of times
// Write is called on it.
type writeCountingDiscard int

func (w *writeCountingDiscard) Write(p []byte) (int, error) {
	*w++
	return len(p), nil
}

// Test for golang.org/issue/5947
func TestWriterReadFromWhileFull(t *testing.T) {
	buf := new(bytes.Buffer)
	w := NewWriterSize(buf, 10)

	// Fill buffer exactly.
	n, err := w.Write([]byte("0123456789"))
	if n != 10 || err != nil {
		t.Fatalf("Write returned (%v, %v), want (10, nil)", n, err)
	}

	// Use ReadFrom to read in some data.
	n2, err := w.ReadFrom(strings.NewReader("abcdef"))
	if n2 != 6 || err != nil {
		t.Fatalf("ReadFrom returned (%v, %v), want (6, nil)", n2, err)
	}
}

type emptyThenNonEmptyReader struct {
	r io.Reader
	n int
}

func (r *emptyThenNonEmptyReader) Read(p []byte) (int, error) {
	if r.n <= 0 {
		return r.r.Read(p)
	}
	r.n--
	return 0, nil
}

// Test for golang.org/issue/7611
func TestWriterReadFromUntilEOF(t *testing.T) {
	buf := new(bytes.Buffer)
	w := NewWriterSize(buf, 5)

	// Partially fill buffer
	n, err := w.Write([]byte("0123"))
	if n != 4 || err != nil {
		t.Fatalf("Write returned (%v, %v), want (4, nil)", n, err)
	}

	// Use ReadFrom to read in some data.
	r := &emptyThenNonEmptyReader{r: strings.NewReader("abcd"), n: 3}
	n2, err := w.ReadFrom(r)
	if n2 != 4 || err != nil {
		t.Fatalf("ReadFrom returned (%v, %v), want (4, nil)", n2, err)
	}
	w.Flush()
	if got, want := string(buf.Bytes()), "0123abcd"; got != want {
		t.Fatalf("buf.Bytes() returned %q, want %q", got, want)
	}
}

func TestWriterReadFromErrNoProgress(t *testing.T) {
	buf := new(bytes.Buffer)
	w := NewWriterSize(buf, 5)

	// Partially fill buffer
	n, err := w.Write([]byte("0123"))
	if n != 4 || err != nil {
		t.Fatalf("Write returned (%v, %v), want (4, nil)", n, err)
	}

	// Use ReadFrom to read in some data.
	r := &emptyThenNonEmptyReader{r: strings.NewReader("abcd"), n: 100}
	n2, err := w.ReadFrom(r)
	if n2 != 0 || err != io.ErrNoProgress {
		t.Fatalf("buf.Bytes() returned (%v, %v), want (0, io.ErrNoProgress)", n2, err)
	}
}

func TestWriterReset(t *testing.T) {
	var buf1, buf2 bytes.Buffer
	w := NewWriter(&buf1)
	w.WriteString("foo")
	w.Reset(&buf2) // and not flushed
	w.WriteString("bar")
	w.Flush()
	if buf1.String() != "" {
		t.Errorf("buf1 = %q; want empty", buf1.String())
	}
	if buf2.String() != "bar" {
		t.Errorf("buf2 = %q; want bar", buf2.String())
	}
}

// An onlyReader only implements io.Reader, no matter what other methods the underlying implementation may have.
type onlyReader struct {
	io.Reader
}

// An onlyWriter only implements io.Writer, no matter what other methods the underlying implementation may have.
type onlyWriter struct {
	io.Writer
}

func BenchmarkWriterCopyOptimal(b *testing.B) {
	// Optimal case is where the underlying writer implements io.ReaderFrom
	srcBuf := bytes.NewBuffer(make([]byte, 8192))
	src := onlyReader{srcBuf}
	dstBuf := new(bytes.Buffer)
	dst := NewWriter(dstBuf)
	for i := 0; i < b.N; i++ {
		srcBuf.Reset()
		dstBuf.Reset()
		dst.Reset(dstBuf)
		io.Copy(dst, src)
	}
}

func BenchmarkWriterCopyUnoptimal(b *testing.B) {
	srcBuf := bytes.NewBuffer(make([]byte, 8192))
	src := onlyReader{srcBuf}
	dstBuf := new(bytes.Buffer)
	dst := NewWriter(onlyWriter{dstBuf})
	for i := 0; i < b.N; i++ {
		srcBuf.Reset()
		dstBuf.Reset()
		dst.Reset(onlyWriter{dstBuf})
		io.Copy(dst, src)
	}
}

func BenchmarkWriterCopyNoReadFrom(b *testing.B) {
	srcBuf := bytes.NewBuffer(make([]byte, 8192))
	src := onlyReader{srcBuf}
	dstBuf := new(bytes.Buffer)
	dstWriter := NewWriter(dstBuf)
	dst := onlyWriter{dstWriter}
	for i := 0; i < b.N; i++ {
		srcBuf.Reset()
		dstBuf.Reset()
		dstWriter.Reset(dstBuf)
		io.Copy(dst, src)
	}
}

func BenchmarkWriterEmpty(b *testing.B) {
	b.ReportAllocs()
	str := strings.Repeat("x", 1<<10)
	bs := []byte(str)
	for i := 0; i < b.N; i++ {
		bw := NewWriter(ioutil.Discard)
		bw.Flush()
		bw.WriteByte('a')
		bw.Flush()
		bw.WriteRune('B')
		bw.Flush()
		bw.Write(bs)
		bw.Flush()
		bw.WriteString(str)
		bw.Flush()
	}
}

func BenchmarkWriterFlush(b *testing.B) {
	b.ReportAllocs()
	bw := NewWriter(ioutil.Discard)
	str := strings.Repeat("x", 50)
	for i := 0; i < b.N; i++ {
		bw.WriteString(str)
		bw.Flush()
	}
}

func BenchmarkLargeConcurrentWrite(b *testing.B) {
	b.ReportAllocs()
	bw := NewWriterSize(ioutil.Discard, 1024)
	str := strings.Repeat("x", 12345)

	var wg sync.WaitGroup
	wg.Add(b.N)
	for i := 0; i < b.N; i++ {
		go func(index int) {
			bw.WriteString(str)
			if index%3 == 0 {
				bw.Flush()
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func BenchmarkSmallConcurrentWrite(b *testing.B) {
	b.ReportAllocs()
	bw := NewWriterSize(ioutil.Discard, 1024)
	str := strings.Repeat("x", 123)

	var wg sync.WaitGroup
	wg.Add(b.N)
	for i := 0; i < b.N; i++ {
		go func(index int) {
			bw.WriteString(str)
			if index%33 == 0 {
				bw.Flush()
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}
