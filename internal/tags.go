package internal

import (
	"encoding/binary"
	"sort"
)

type Tag struct {
	Name  string
	Value string
}

func NewTag(name string, value string) *Tag {
	return &Tag{
		Name:  name,
		Value: value,
	}
}

func NewTags() Tags {
	return Tags{}
}

type Tags []*Tag

func (tags Tags) Len() int {
	return len(tags)
}
func (tags Tags) Less(i, j int) bool {
	first, second := tags[i], tags[j]

	if first.Name == second.Name {
		return first.Value < second.Value
	}
	return first.Name < second.Name
}
func (tags Tags) Swap(i, j int) {
	tags[i], tags[j] = tags[j], tags[i]
}

func (tags Tags) Sort() {
	sort.Sort(tags)
}

func (tags Tags) Size() uint64 {
	var total uint64 = 0
	for _, tag := range tags {
		nameLen := uint64(len(tag.Name))
		valueLen := uint64(len(tag.Value))
		total += 8 + nameLen + 8 + valueLen
	}
	return total
}

func (tags Tags) Serialize() []byte {
	buffer := make([]byte, 0)

	for _, tag := range tags {
		nameBytes := []byte(tag.Name)
		nameLen := make([]byte, 8)
		binary.BigEndian.PutUint64(nameLen, uint64(len(nameBytes)))
		buffer = append(buffer, nameLen...)
		buffer = append(buffer, nameBytes...)

		valueBytes := []byte(tag.Value)
		valueLen := make([]byte, 8)
		binary.BigEndian.PutUint64(valueLen, uint64(len(valueBytes)))
		buffer = append(buffer, valueLen...)
		buffer = append(buffer, valueBytes...)
	}

	return buffer
}

func DeserializeTags(data []byte, numTags uint64) (Tags, int) {
	offset := 0
	tags := NewTags()

	for i := uint64(0); i < numTags; i++ {
		nameLen := binary.BigEndian.Uint64(data[offset:])
		offset += 8

		name := string(data[offset : offset+int(nameLen)])
		offset += int(nameLen)

		valueLen := binary.BigEndian.Uint64(data[offset:])
		offset += 8

		value := string(data[offset : offset+int(valueLen)])
		offset += int(valueLen)

		tags = append(tags, &Tag{
			Name:  name,
			Value: value,
		})
	}

	return tags, offset
}
