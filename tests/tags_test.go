package tests

import (
	"reflect"
	"testing"
	"time-series-engine/internal"
)

func TestNewTag(t *testing.T) {
	tag := internal.NewTag("env", "prod")
	if tag == nil {
		t.Fatal("Expected non-nil Tag")
	}
	if tag.Name != "env" || tag.Value != "prod" {
		t.Errorf("Expected tag with Name=env, Value=prod, got %v", tag)
	}
}

func TestTagsSort(t *testing.T) {
	tags := internal.Tags{
		*internal.NewTag("z", "3"),
		*internal.NewTag("a", "2"),
		*internal.NewTag("a", "1"),
		*internal.NewTag("b", "1"),
	}

	tags.Sort()

	expected := internal.Tags{
		*internal.NewTag("a", "1"),
		*internal.NewTag("a", "2"),
		*internal.NewTag("b", "1"),
		*internal.NewTag("z", "3"),
	}

	if !reflect.DeepEqual(tags, expected) {
		t.Errorf("Tags.Sort() failed.\nExpected: %v\nGot:      %v", expected, tags)
	}
}

func TestTimeSeriesKey(t *testing.T) {
	tags := internal.Tags{
		*internal.NewTag("region", "us-west"),
		*internal.NewTag("env", "staging"),
	}
	ts := internal.NewTimeSeries("cpu_usage", tags)

	// Key() sorts tags internally
	key := ts.Key()
	expectedKey := "cpu_usage|env=staging|region=us-west"

	if key != expectedKey {
		t.Errorf("TimeSeries.Key() expected %q, got %q", expectedKey, key)
	}
}
