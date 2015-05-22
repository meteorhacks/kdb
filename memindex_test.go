package kdb

import (
	"reflect"
	"testing"
)

func TestCreateIndex(t *testing.T) {
	orderedKeys := []string{"appId", "host"}
	index := NewMemIndex(orderedKeys)
	if !reflect.DeepEqual(index.orderedKeys, orderedKeys) {
		t.Error("orderedKey must be set")
	}

	if index.root == nil {
		t.Error("index root must be set")
	}
}

func TestAddItemToIndex(t *testing.T) {
	index := NewMemIndex([]string{"appId", "host"})

	el := map[string]string{"appId": "kadira", "host": "h1"}
	var blockPosition int64 = 10
	index.AddItem(el, blockPosition)

	addedItem := index.root.Map["kadira"].Map["h1"]

	if addedItem.BlockPosition != blockPosition {
		t.Error("block position must be present")
	}

	expectedValues := []string{"kadira", "h1"}
	if !reflect.DeepEqual(addedItem.Values, expectedValues) {
		t.Error("values are incorrect")
	}
}

func TestAddItemToIndexOverride(t *testing.T) {
	index := NewMemIndex([]string{"appId", "host"})

	el := map[string]string{"appId": "kadira", "host": "h1"}
	var blockPosition int64 = 10
	index.AddItem(el, blockPosition)

	blockPosition = 100
	index.AddItem(el, blockPosition)

	addedItem := index.root.Map["kadira"].Map["h1"]

	if addedItem.BlockPosition != blockPosition {
		t.Error("block position does not get overridden")
	}
}
