package tests

import (
	"reflect"
	"testing"
	"time-series-engine/internal"
)

func TestNewTag(t *testing.T) {
	tag := internal.NewTag("host", "server1")
	if tag.Name != "host" {
		t.Errorf("expected Name to be host, got %s", tag.Name)
	}
	if tag.Value != "server1" {
		t.Errorf("expected Value to be server1, got %s", tag.Value)
	}
}

func TestTagsSort(t *testing.T) {
	tags := internal.Tags{
		{Name: "region", Value: "eu"},
		{Name: "host", Value: "server1"},
		{Name: "env", Value: "prod"},
	}

	expected := internal.Tags{
		{Name: "env", Value: "prod"},
		{Name: "host", Value: "server1"},
		{Name: "region", Value: "eu"},
	}

	tags.Sort()

	if !reflect.DeepEqual(tags, expected) {
		t.Errorf("expected sorted tags %v, got %v", expected, tags)
	}
}
