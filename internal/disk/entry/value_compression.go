package entry

import (
	"math/bits"
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

func (vc *ValueCompressor) CalculateNext(e Entry, count uint64) *CompressedData {
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

// Case1 for when the current value is same as the last value.
// This case is represented with a single 0 bit
func (vc *ValueCompressor) Case1() (uint64, int) {
	return 0, 1
}

// Case2 for when the current value is different from the last value
// and has same amount of leading & trailing zeros as the last value.
// This case is represented with bits 10, followed up with xor bits
// (e.g. 101001101, where 10 represents case2 and 1001101 are xor bits)
func (vc *ValueCompressor) Case2(xor uint64, leading, trailing int) (uint64, int) {
	const mask = uint64(2) << 62 // mask is 10000000...

	numBitsInXor := 64 - leading - trailing
	xorShifted := (xor >> trailing) << (62 - numBitsInXor) // shift xor bits right behind mask
	result := mask | xorShifted
	return result, numBitsInXor + 2
}

/*
Case3 for when the current value is different from the last value
and amount of leading or trailing zeros is different from the last value.
This case is represented with bits 11, followed up by:
- 5 bits integer representing amount of leading zeros,
- 6 bits integer representing amount of bits in xor value
- xor bits

	e.g. 1101000010001111111110110110011
	11 represents case3,
	01000 are bits representing num of leading zeros,
	010001 are bits representing num of xor bits,
	111111110110110011 are xor bits
*/
func (vc *ValueCompressor) Case3(xor uint64, leading, trailing int) (uint64, int) {
	const mask = uint64(3) << 62 // mask is 11000000...

	numBitsInXor := 64 - leading - trailing

	// num of leading zeros uses 5 bits
	leadingShifted := uint64(leading) << 57 // 64-2-5
	// num of bits in xor uses 6 bits
	xorSizeShifted := uint64(numBitsInXor) << 51 // 64-2-5-6

	xorShifted := (xor >> trailing) << (51 - numBitsInXor)

	result := mask | leadingShifted | xorSizeShifted | xorShifted
	return result, numBitsInXor + 13
}

func (vc *ValueCompressor) Update(newLastValue uint64, newLastLeading, newLastTrailing int) {
	vc.lastValue = newLastValue
	vc.lastLeading = newLastLeading
	vc.lastTrailing = newLastTrailing
}
