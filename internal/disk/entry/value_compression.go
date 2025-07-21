package entry

/*
	Floating-point compression based on Facebook's Gorilla time-series encoding.

	Three cases are used to compress the XOR between the current and previous value:

	Case 1: Value is the same as the previous one.
	- Represented by a single '0' bit.
	- No additional bits are stored.

	Case 2: Value differs from the previous, but the number of leading and trailing zeros
	in the XOR result is the same as in the previous difference.
	- Represented by the bit pattern '10'.
	- Followed directly by the meaningful XOR bits.

	Case 3: Value differs from the previous, and the number of leading or trailing zeros
	has changed compared to the previous XOR result.
	- Represented by the bit pattern '11'.
	- Followed by:
		- 5 bits: number of leading zeros,
		- 6 bits: number of meaningful XOR bits,
		- XOR bits of specified length.

	Decompression reconstructs the current value as:
		current = previous ^ (XOR bits << number of trailing zeros)
*/

import (
	"math"
	"math/bits"
	"time-series-engine/internal"
)

type CompressedData struct {
	Value     uint64
	ValueSize int
	Leading   int
	Trailing  int
}

func NewCompressedData(value uint64, valueSize, leading, trailing int) *CompressedData {
	return &CompressedData{
		Value:     value,
		ValueSize: valueSize,
		Leading:   leading,
		Trailing:  trailing,
	}
}

func (cd *CompressedData) Update(value uint64, valueSize, leading, trailing int) {
	cd.Value = value
	cd.ValueSize = valueSize
	cd.Leading = leading
	cd.Trailing = trailing
}

type ValueCompressor struct {
	lastValue    uint64
	lastLeading  int
	lastTrailing int
}

func NewValueCompressor() *ValueCompressor {
	return &ValueCompressor{}
}

func (vc *ValueCompressor) CompressNextEntry(e Entry, count uint64) *CompressedData {
	ve, ok := e.(*ValueEntry)
	if !ok {
		return nil
	}

	var size = 0
	var result uint64 = 0

	if count == 0 { // if first value to be written on page
		return NewCompressedData(ve.Value, 64, 0, 0)
	} else {
		xor := ve.Value ^ vc.lastValue
		leading := bits.LeadingZeros64(xor)
		trailing := bits.TrailingZeros64(xor)
		if xor == 0 {
			result, size = vc.Case1()
		} else if leading == vc.lastLeading && trailing == vc.lastTrailing {
			result, size = vc.Case2(xor, leading, trailing)
		} else {
			result, size = vc.Case3(xor, leading, trailing)
		}
		return NewCompressedData(result, size, leading, trailing)
	}
}

func (vc *ValueCompressor) Case1() (uint64, int) {
	return 0, 1
}

func (vc *ValueCompressor) Case2(xor uint64, leading, trailing int) (uint64, int) {
	const mask = uint64(2) << 62 // mask is 10000000...

	xorLen := 64 - leading - trailing
	xorShifted := (xor >> trailing) << (62 - xorLen) // shift xor bits right behind mask
	result := mask | xorShifted
	return result, xorLen + 2
}

func (vc *ValueCompressor) Case3(xor uint64, leading, trailing int) (uint64, int) {
	const mask = uint64(3) << 62 // mask is 11000000...

	xorLen := 64 - leading - trailing

	// num of leading zeros uses 5 bits
	leadingShifted := uint64(leading) << 57 // 64-2-5
	// num of bits in xor uses 6 bits
	xorSizeShifted := uint64(xorLen) << 51 // 64-2-5-6

	xorShifted := (xor >> trailing) << (51 - xorLen)

	result := mask | leadingShifted | xorSizeShifted | xorShifted
	return result, xorLen + 13
}

func (vc *ValueCompressor) Update(newLastValue uint64, newLastLeading, newLastTrailing int) {
	vc.lastValue = newLastValue
	vc.lastLeading = newLastLeading
	vc.lastTrailing = newLastTrailing
}

type ValueDecompressor struct {
	bitReader    *internal.BitReader
	lastValue    uint64
	lastLeading  int
	lastTrailing int
}

func NewValueDecompressor(bitReader *internal.BitReader) *ValueDecompressor {
	return &ValueDecompressor{
		bitReader: bitReader,
	}
}

func (vd *ValueDecompressor) DecompressNextEntry(count uint64) *ValueEntry {
	if count == 0 {
		readBits := vd.bitReader.ReadBits(64)
		vd.lastValue = readBits
		vd.lastLeading = 0
		vd.lastTrailing = 0
		return NewValueEntry(math.Float64frombits(readBits))
	}

	controlBit := vd.bitReader.ReadBits(1)
	if controlBit == 0 { // Case 1
		return NewValueEntry(math.Float64frombits(vd.lastValue))
	}
	secondBit := vd.bitReader.ReadBits(1)
	if secondBit == 0 {
		return vd.Case2()
	}
	if secondBit == 1 {
		return vd.Case3()
	}
	return nil
}

func (vd *ValueDecompressor) Case2() *ValueEntry {
	xorLen := 64 - vd.lastLeading - vd.lastTrailing
	xorBits := vd.bitReader.ReadBits(xorLen)
	xorBits <<= vd.lastTrailing

	value := vd.lastValue ^ xorBits
	vd.lastValue = value
	return NewValueEntry(math.Float64frombits(value))
}

func (vd *ValueDecompressor) Case3() *ValueEntry {
	leading := vd.bitReader.ReadBits(5)
	xorLen := vd.bitReader.ReadBits(6)
	xorBits := vd.bitReader.ReadBits(int(xorLen))
	trailing := 64 - leading - xorLen
	xorBits <<= trailing

	value := vd.lastValue ^ xorBits
	vd.lastValue = value
	vd.lastLeading = int(leading)
	vd.lastTrailing = int(trailing)
	return NewValueEntry(math.Float64frombits(value))
}
