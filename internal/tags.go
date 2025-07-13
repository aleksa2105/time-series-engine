package internal

import (
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

type Tags []Tag

func (tags Tags) Len() int {
	return len(tags)
}

func (tags Tags) Less(i, j int) bool {
	if tags[i].Name < tags[j].Name {
		return true
	}
	if tags[i].Name > tags[j].Name {
		return false
	}

	return tags[i].Value < tags[j].Value
}

func (tags Tags) Swap(i, j int) {
	tags[i], tags[j] = tags[j], tags[i]
}

func (tags Tags) Sort() {
	sort.Sort(tags)
}
