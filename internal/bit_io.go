package internal

import (
	"errors"
)

// Seek whence values.
const (
	SeekStart   = 0 // seek relative to the origin of the file
	SeekCurrent = 1 // seek relative to the current offset
	SeekEnd     = 2 // seek relative to the end
)

type BitWriter struct {
	buf     []byte
	curr    byte
	nBits   uint8 // num of bits written in curr
	currOff int64 // current position in buf(in bits)
}

func NewBitWriter(bufSize uint64) *BitWriter {
	return &BitWriter{
		buf:   make([]byte, 0, bufSize),
		curr:  byte(0),
		nBits: 0,
	}
}

func (w *BitWriter) WriteBit(bit uint8) error {
	if w.currOff+1 > w.MaxBitSize() {
		return errors.New("bits out of range")
	}
	if bit == 1 {
		w.curr |= 1 << (7 - w.nBits)
	}
	w.nBits++
	w.currOff++
	if w.nBits == 8 {
		w.Flush()
	}
	return nil
}

func (w *BitWriter) WriteBits(bits uint64, numBits int64) {
	for i := int64(0); i < numBits; i++ {
		bit := (bits >> (63 - i)) & 1
		_ = w.WriteBit(uint8(bit))
	}
}

func (w *BitWriter) Flush() {
	if w.nBits > 0 {
		w.buf = append(w.buf, w.curr)
		w.curr = byte(0)
		w.nBits = 0
	}
}

func (w *BitWriter) BitSize() int64 {
	return int64(len(w.buf)) * 8
}

func (w *BitWriter) MaxBitSize() int64 {
	return int64(cap(w.buf)+1) * 8
}

// Seek returns the new offset relative to the start of the
// file or an error, if any.
func (w *BitWriter) Seek(offset int64, whence int) (int64, error) {
	var relOff int64

	switch whence {
	case SeekStart:
		if offset < 0 || offset > w.BitSize() {
			return w.currOff, errors.New("offset out of range")
		}
		relOff = offset

	case SeekCurrent:
		relOff = w.currOff + offset
		if relOff < 0 || relOff > w.BitSize() {
			return w.currOff, errors.New("offset out of range")
		}

	case SeekEnd:
		relOff = w.BitSize() + offset
		if relOff < 0 || relOff > w.BitSize() {
			return w.currOff, errors.New("offset out of range")
		}

	default:
		return w.currOff, errors.New("invalid whence value")
	}

	w.currOff = relOff
	byteIdx := relOff / 8
	if byteIdx >= int64(len(w.buf)) {
		w.curr = byte(0)
		w.nBits = 0
		return w.currOff, nil
	}
	w.curr = w.buf[byteIdx]
	w.nBits = uint8(relOff % 8)

	return w.currOff, nil
}

func (w *BitWriter) Bytes() []byte {
	return w.buf
}

type BitReader struct {
	buf     []byte
	curr    byte
	nBits   uint8 // num of bits read from curr
	currOff int64 // current position in buf(in bits)
}

func NewBitReader(buf []byte) *BitReader {
	return &BitReader{
		buf:     buf,
		curr:    buf[0],
		currOff: 0,
		nBits:   0,
	}
}

func (r *BitReader) ReadBit() (uint8, error) {
	if r.currOff+1 > r.BitSize() {
		return 0, errors.New("bits out of range")
	}
	const readHighestBitMask = uint8(1) << 7
	var currBit = r.curr & readHighestBitMask
	if currBit != 0 {
		currBit = 1
	}
	r.curr <<= 1
	r.nBits++
	r.currOff++
	if r.nBits == 8 {
		r.loadNextByte()
	}
	return currBit, nil
}

func (r *BitReader) ReadBits(numBits int) (uint64, error) {
	var result uint64 = 0
	for i := 0; i < numBits; i++ {
		result <<= 1
		bit, err := r.ReadBit()
		if err != nil {
			return 0, err
		}
		result |= uint64(bit)
	}
	return result, nil
}

func (r *BitReader) loadNextByte() {
	byteIdx := r.currOff / 8
	if byteIdx >= int64(len(r.buf)) {
		r.curr = 0
		return
	}
	r.curr = r.buf[byteIdx]
	r.nBits = 0
}

// Seek returns the new offset relative to the start of the
// file or an error, if any.
func (r *BitReader) Seek(offset int64, whence int) (int64, error) {
	var relOff int64

	switch whence {
	case SeekStart:
		if offset < 0 || offset > r.BitSize() {
			return r.currOff, errors.New("offset out of range")
		}
		relOff = offset

	case SeekCurrent:
		relOff = r.currOff + offset
		if relOff < 0 || relOff > r.BitSize() {
			return r.currOff, errors.New("offset out of range")
		}

	case SeekEnd:
		relOff = r.BitSize() + offset
		if relOff < 0 || relOff > r.BitSize() {
			return r.currOff, errors.New("offset out of range")
		}

	default:
		return r.currOff, errors.New("invalid whence value")
	}

	r.currOff = relOff
	byteIdx := relOff / 8
	if byteIdx >= int64(len(r.buf)) {
		r.curr = byte(0)
		r.nBits = 0
		return r.currOff, nil
	}
	r.curr = r.buf[byteIdx]
	r.nBits = uint8(relOff % 8)

	return r.currOff, nil
}

func (r *BitReader) Bytes() []byte {
	return r.buf
}
func (r *BitReader) BitSize() int64 {
	return int64(len(r.buf)) * 8
}

func (r *BitReader) MaxBitSize() int64 {
	return int64(cap(r.buf)) * 8
}
