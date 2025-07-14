package chunk

import (
	"encoding/binary"
)

type Dictionary struct {
	IdToKey map[uint64]string
	KeyToId map[string]uint64
}

func NewDictionary() *Dictionary {
	return &Dictionary{
		IdToKey: make(map[uint64]string),
		KeyToId: make(map[string]uint64),
	}
}

// Serialize format
// Id variable size
// keySize variable
// key
func (d *Dictionary) Serialize() []byte {
	allBytes := make([]byte, 0)

	for id, key := range d.IdToKey {
		idBytes := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(idBytes, id)
		allBytes = append(allBytes, idBytes[:n]...)

		keySizeBytes := make([]byte, 10)
		n = binary.PutUvarint(keySizeBytes, uint64(len(key)))
		allBytes = append(allBytes, keySizeBytes[:n]...)

		allBytes = append(allBytes, key...)
	}

	return allBytes
}

func Deserialize(bytes []byte) *Dictionary {
	var offset uint64 = 0

	keyToId := make(map[string]uint64)
	idToKey := make(map[uint64]string)

	for offset < uint64(len(bytes)) {
		id, n := binary.Uvarint(bytes[offset:])
		offset += uint64(n)

		keySize, n := binary.Uvarint(bytes[offset:])
		offset += uint64(n)

		key := string(bytes[offset : offset+keySize])
		offset += keySize

		keyToId[key] = id
		idToKey[id] = key
	}

	return &Dictionary{
		KeyToId: keyToId,
		IdToKey: idToKey,
	}
}
