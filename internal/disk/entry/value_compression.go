package entry

/*
	Floating-point compression based on Facebook's Gorilla time-series encoding.

	Three cases are used to compress the XOR between the current and previous value:

	Case 1: Value is the same as the previous one.
	- Represented by a single '00' bit.
	- No additional bits are stored.

	Case 2: Value differs from the previous, but the number of leading and trailing zeros
	in the XOR result is the same as in the previous difference.
	- Represented by the bit pattern '01'.
	- Followed directly by the meaningful XOR bits.

	Case 3: Value differs from the previous, and the number of leading or trailing zeros
	has changed compared to the previous XOR result.
	- Represented by the bit pattern '10'.
	- Followed by:
		- 6 bits: number of leading zeros,
		- 6 bits: number of meaningful XOR bits,
		- XOR bits of specified length.

	Case 4: Value differs greatly from the previous, and the number of XOR bits is larger than maxXorLen
	- Represented by the bit pattern '11'
	- Followed by scaled value

	Decompression reconstructs the current value as:
		current = previous ^ (XOR bits << number of trailing zeros)
*/

import (
	"math"
	"math/bits"
	"time-series-engine/internal"
)

const scaleFactor = 100000
const maxXorLen = 50

type CompressedData struct {
	Value      uint64
	ValueSize  int
	Compressed bool
}

func NewCompressedData(value uint64, valueSize int, compressed bool) *CompressedData {
	return &CompressedData{
		Value:      value,
		ValueSize:  valueSize,
		Compressed: compressed,
	}
}

func (cd *CompressedData) Update(value uint64, valueSize int) {
	cd.Value = value
	cd.ValueSize = valueSize
}

type ValueCompressor struct {
	lastValue    uint64
	lastLeading  int
	lastTrailing int
}

func NewValueCompressor() *ValueCompressor {
	return &ValueCompressor{}
}

func (vc *ValueCompressor) CompressNextValue(value float64, count uint64) *CompressedData {
	var (
		size       = 0
		result     = uint64(0)
		compressed = false
	)

	valueScaled := scale(value)

	if count == 0 { // if first value to be written on page
		vc.Update(valueScaled, 0, 0)
		return NewCompressedData(valueScaled, 64, false)
	} else {
		xor := valueScaled ^ vc.lastValue
		leading := bits.LeadingZeros64(xor)
		trailing := bits.TrailingZeros64(xor)
		xorLen := 64 - leading - trailing

		if xor == 0 { // Case 1
			result, size, compressed = 0, 2, true
		} else if xorLen > maxXorLen { // Case 4
			result, size, compressed = valueScaled, 64, false
		} else if leading == vc.lastLeading && trailing == vc.lastTrailing {
			result, size, compressed = vc.Case2(xor, leading, trailing)
		} else {
			result, size, compressed = vc.Case3(xor, leading, trailing)
		}
		vc.Update(valueScaled, leading, trailing)
		return NewCompressedData(result, size, compressed)
	}
}

func (vc *ValueCompressor) Case2(xor uint64, leading, trailing int) (uint64, int, bool) {
	const mask = uint64(1) << 62 // mask is 01000000...

	xorLen := 64 - leading - trailing
	xorShifted := (xor >> trailing) << (62 - xorLen) // shift xor bits right behind mask
	result := mask | xorShifted
	return result, xorLen + 2, true
}

func (vc *ValueCompressor) Case3(xor uint64, leading, trailing int) (uint64, int, bool) {
	const mask = uint64(2) << 62 // mask is 10000000...

	xorLen := 64 - leading - trailing

	leadingShifted := uint64(leading) << 56 // 64-2-6
	xorSizeShifted := uint64(xorLen) << 50  // 64-2-6-6

	xorShifted := (xor >> trailing) << (50 - xorLen)

	result := mask | leadingShifted | xorSizeShifted | xorShifted
	return result, xorLen + 14, true
}

func (vc *ValueCompressor) Update(newLastValue uint64, newLastLeading, newLastTrailing int) {
	vc.lastValue = newLastValue
	vc.lastLeading = newLastLeading
	vc.lastTrailing = newLastTrailing
}

func scale(value float64) uint64 {
	scaled := math.Trunc(value * scaleFactor)
	return math.Float64bits(scaled)
}

func downScale(value uint64) float64 {
	scaled := math.Float64frombits(value)
	return scaled / scaleFactor
}

type ValueReconstructor struct {
	bitReader    *internal.BitReader
	lastValue    uint64
	lastLeading  int
	lastTrailing int
}

func NewValueReconstructor(bytes []byte) *ValueReconstructor {
	return &ValueReconstructor{
		bitReader: internal.NewBitReader(bytes),
	}
}

func (vr *ValueReconstructor) ReconstructNextValue() *ValueEntry {
	controlBits := vr.bitReader.ReadBits(2)
	if controlBits == 0 { // Case 1
		cd := NewCompressedData(0, 2, true)
		return NewValueEntry(downScale(vr.lastValue), cd)
	}
	if controlBits == 1 {
		return vr.Case2()
	}
	if controlBits == 2 {
		return vr.Case3()
	}
	if controlBits == 3 {
		return vr.Case4()
	}
	return nil
}

func (vr *ValueReconstructor) Case2() *ValueEntry {
	xorLen := 64 - vr.lastLeading - vr.lastTrailing
	xor := vr.bitReader.ReadBits(xorLen)
	xor <<= vr.lastTrailing

	value := vr.lastValue ^ xor
	vr.Update(value, vr.lastLeading, vr.lastTrailing)

	const mask = uint64(1) << 62 // mask is 01000000...

	xorShifted := (xor >> vr.lastTrailing) << (62 - xorLen) // shift xor bits right behind mask
	cmpVal := mask | xorShifted
	cmpValSize := xorLen + 2
	cd := NewCompressedData(cmpVal, cmpValSize, true)

	return NewValueEntry(downScale(value), cd)
}

func (vr *ValueReconstructor) Case3() *ValueEntry {
	leading := vr.bitReader.ReadBits(6)
	xorLen := vr.bitReader.ReadBits(6)
	xor := vr.bitReader.ReadBits(int(xorLen))
	trailing := 64 - leading - xorLen
	xor <<= trailing

	value := vr.lastValue ^ xor
	vr.Update(value, int(leading), int(trailing))

	leadingShifted := leading << 56 // 64-2-6
	xorLenShifted := xorLen << 50   // 64-2-6-6

	const mask = uint64(2) << 62 // mask is 10000000...

	xorShifted := (xor >> trailing) << (50 - xorLen)
	cmpVal := mask | leadingShifted | xorLenShifted | xorShifted
	cmpValSize := xorLen + 14
	cd := NewCompressedData(cmpVal, int(cmpValSize), true)

	return NewValueEntry(downScale(value), cd)
}

func (vr *ValueReconstructor) Case4() *ValueEntry {
	value := vr.bitReader.ReadBits(64)
	vr.Update(value, 0, 0)
	cd := NewCompressedData(value, 64, false)

	return NewValueEntry(downScale(value), cd)
}

func (vr *ValueReconstructor) Update(lastValue uint64, lastLeading, lastTrailing int) {
	vr.lastValue = lastValue
	vr.lastLeading = lastLeading
	vr.lastTrailing = lastTrailing
}

func (vr *ValueReconstructor) LastValue() uint64 {
	return vr.lastValue
}

func (vr *ValueReconstructor) LastLeading() int {
	return vr.lastLeading
}

func (vr *ValueReconstructor) LastTrailing() int {
	return vr.lastTrailing
}
