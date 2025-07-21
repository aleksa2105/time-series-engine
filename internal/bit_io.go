package internal

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
			w.flush()
		}
	}
}

func (w *BitWriter) WriteBit(bit uint8) {
	if bit == 1 {
		w.curr |= 1 << (7 - w.nBits)
	}
	w.nBits++
	if w.nBits == 8 {
		w.flush()
	}
}

func (w *BitWriter) flush() {
	if w.nBits > 0 {
		w.buf = append(w.buf, w.curr)
		w.curr = byte(0)
		w.nBits = 0
	}
}

func (w *BitWriter) Bytes() []byte {
	return w.buf
}

type BitReader struct {
	buf         []byte
	curr        byte
	currByteIdx int
	nBits       uint8 // num of bits read from curr
}

func NewBitReader(buf []byte) *BitReader {
	return &BitReader{
		buf:         buf,
		curr:        buf[0],
		currByteIdx: 0,
		nBits:       0,
	}
}

func (r *BitReader) ReadCurrBit() uint8 {
	const readHighestBitMask = uint8(1) << 7
	var currBit = r.curr & readHighestBitMask
	if currBit != 0 {
		currBit = 1
	}
	r.curr <<= 1
	r.nBits++
	if r.nBits == 8 {
		r.loadNextByte()
	}
	return currBit
}

func (r *BitReader) loadNextByte() {
	r.currByteIdx++
	r.curr = r.buf[r.currByteIdx]
	r.nBits = 0
}

func (r *BitReader) ReadBits(numBits int) uint64 {
	var result uint64 = 0
	for i := 0; i < numBits; i++ {
		result <<= 1
		result |= uint64(r.ReadCurrBit())
	}
	return result
}
