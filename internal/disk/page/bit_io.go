package page

type BitWriter struct {
	buf   []byte
	curr  byte
	nBits uint8 // num of bits written in curr
}

func NewBitWriter(bufSize uint64) *BitWriter {
	return &BitWriter{
		buf:   make([]byte, 0, bufSize),
		curr:  byte(0),
		nBits: 0,
	}
}

func (w *BitWriter) WriteBits(bits uint64, numBits int) {
	for i := numBits - 1; i >= 0; i-- {
		bit := (bits >> i) & 1
		if bit == 1 {
			w.curr |= 1 << (7 - w.nBits)
		}
		w.nBits++
		if w.nBits == 8 {
			w.Flush()
		}
	}
}

func (w *BitWriter) WriteBit(bit uint8) {
	if bit == 1 {
		w.curr |= 1 << (7 - w.nBits)
	}
	w.nBits++
	if w.nBits == 8 {
		w.Flush()
	}
}

func (w *BitWriter) Flush() {
	if w.nBits > 0 {
		w.buf = append(w.buf, w.curr)
		w.curr = byte(0)
		w.nBits = 0
	}
	// krsto
}

func (w *BitWriter) Bytes() []byte {
	return w.buf
}

type BitReader struct {
}
