package entry

import "encoding/binary"

type TSCompressedData struct {
	Bytes []byte
}

func NewTSCompressedData(bytes []byte) *TSCompressedData {
	return &TSCompressedData{
		Bytes: bytes,
	}
}

type TimestampCompressor struct {
	lastValue uint64
}

func NewTimestampCompressor() *TimestampCompressor {
	return &TimestampCompressor{}
}

func (tsc *TimestampCompressor) CompressNext(timeStamp uint64, count uint64) *TSCompressedData {
	var compressedBytes []byte
	if count == 0 {
		compressedBytes = serializeTimestamp(timeStamp)
	} else {
		delta := timeStamp - tsc.lastValue
		compressedBytes = serializeTimestamp(delta)
	}
	tsc.Update(timeStamp)
	return NewTSCompressedData(compressedBytes)
}

func serializeTimestamp(timestamp uint64) []byte {
	bytes := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(bytes, timestamp)
	return bytes[:n]
}

func (tsc *TimestampCompressor) Update(lastValue uint64) {
	tsc.lastValue = lastValue
}

type TimestampReconstructor struct {
	bytes     []byte
	offset    uint64
	lastValue uint64
}

func NewTimestampReconstructor(bytes []byte) *TimestampReconstructor {
	return &TimestampReconstructor{
		bytes: bytes,
	}
}

func (tsr *TimestampReconstructor) ReconstructNext() *TimestampEntry {
	if tsr.offset >= uint64(len(tsr.bytes)) {
		return nil
	}
	
	timestamp, bytesRead := deserializeTimestamp(tsr.bytes[tsr.offset:])
	if bytesRead <= 0 {
		return nil
	}
	timestamp += tsr.lastValue // add delta

	cd := NewTSCompressedData(tsr.bytes[tsr.offset : tsr.offset+bytesRead])
	tsr.Update(timestamp, bytesRead)
	return NewTimestampEntry(timestamp, cd)
}

func (tsr *TimestampReconstructor) Update(lastValue uint64, bytesRead uint64) {
	tsr.lastValue = lastValue
	tsr.offset += bytesRead
}

// deserializeTimestamp returns uint64 timestamp and number of bytes read
func deserializeTimestamp(bytes []byte) (uint64, uint64) {
	value, n := binary.Uvarint(bytes)
	if n <= 0 {
		return 0, 0
	}
	return value, uint64(n)
}

func (tsr *TimestampReconstructor) LastValue() uint64 {
	return tsr.lastValue
}
