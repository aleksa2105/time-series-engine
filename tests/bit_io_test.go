package tests

import (
	"testing"
	"time-series-engine/internal"
)

func TestBitWriterAndReader(t *testing.T) {
	w := internal.NewBitWriter(10)

	// Upisujemo 10 bitova: 1100101011
	var bitsToWrite uint64 = 0b1100101011000000000000000000000000000000000000000000000000000000
	w.WriteBits(bitsToWrite, 10)

	// Flush da se sve prebaci u buffer
	w.Flush()
	buf := w.Bytes()

	// Proveri da buffer nije prazan
	if len(buf) == 0 {
		t.Fatal("buffer is empty after writing bits")
	}

	r := internal.NewBitReader(buf)

	readBits, err := r.ReadBits(10)
	if err != nil {
		t.Fatalf("failed to read bits: %v", err)
	}
	expected := bitsToWrite >> 54 // jer pišemo prvih 10 bitova od 64
	if readBits != expected {
		t.Errorf("read bits %b, expected %b", readBits, expected)
	}
}

func TestSeek(t *testing.T) {
	w := internal.NewBitWriter(10)
	w.WriteBits(0b10101100_11100000_00000000_00000000_00000000_00000000_00000000_00000000, 8)
	w.Flush()

	_, err := w.Seek(4, internal.SeekStart)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if w.currOff != 4 {
		t.Errorf("expected offset 4, got %d", w.currOff)
	}

	_, err = w.Seek(-1, internal.SeekStart)
	if err == nil {
		t.Errorf("expected error for negative offset, got nil")
	}

	_, err = w.Seek(100, internal.SeekEnd)
	if err == nil {
		t.Errorf("expected error for out-of-range offset, got nil")
	}
}

func TestReaderSeek(t *testing.T) {
	w := internal.NewBitWriter(10)
	w.WriteBits(0b10101010_00000000_00000000_00000000_00000000_00000000_00000000_00000000, 8)
	w.Flush()
	r := internal.NewBitReader(w.Bytes())

	_, err := r.Seek(2, internal.SeekStart)
	if err != nil {
		t.Fatalf("SeekStart failed: %v", err)
	}

	bit, err := r.ReadBit()
	if err != nil {
		t.Fatalf("ReadBit failed: %v", err)
	}
	if bit != 1 {
		t.Errorf("expected bit 1 at offset 2, got %d", bit)
	}

	_, err = r.Seek(-1, internal.SeekStart)
	if err == nil {
		t.Errorf("expected error for negative relative offset")
	}

	_, err = r.Seek(100, internal.SeekEnd)
	if err == nil {
		t.Errorf("expected error for large SeekEnd")
	}
}

func TestWriteAndReadSingleBits(t *testing.T) {
	w := internal.NewBitWriter(10)
	w.WriteBit(1)
	w.WriteBit(0)
	w.WriteBit(1)
	w.WriteBit(1)
	w.WriteBit(0)
	w.Flush()

	r := internal.NewBitReader(w.Bytes())
	bits := []uint8{1, 0, 1, 1, 0}

	for i, expected := range bits {
		b, err := r.ReadBit()
		if err != nil {
			t.Fatalf("ReadBit error at %d: %v", i, err)
		}
		if b != expected {
			t.Errorf("bit mismatch at %d: got %d, want %d", i, b, expected)
		}
	}
}

func TestReadPastEOF(t *testing.T) {
	w := internal.NewBitWriter(10)
	w.WriteBits(0b10101010_00000000_00000000_00000000_00000000_00000000_00000000_00000000, 8)
	w.Flush()

	r := internal.NewBitReader(w.Bytes())

	_, _ = r.ReadBits(8)
	_, err := r.ReadBit()
	if err == nil {
		t.Error("expected error after reading past EOF")
	}
}
