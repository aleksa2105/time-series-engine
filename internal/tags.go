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
