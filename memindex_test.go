package kdb

import (
	"reflect"
	"sort"
	"testing"
)

// Create Index

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

// Add to Index

func TestAddItemToIndex(t *testing.T) {
	index := NewMemIndex([]string{"appId", "host"})

	item := map[string]string{"appId": "kadira", "host": "h1"}
	var blockPosition int64 = 10
	index.AddItem(item, blockPosition)

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

	item := map[string]string{"appId": "kadira", "host": "h1"}
	var blockPosition int64 = 10
	index.AddItem(item, blockPosition)

	blockPosition = 100
	index.AddItem(item, blockPosition)

	addedItem := index.root.Map["kadira"].Map["h1"]

	if addedItem.BlockPosition != blockPosition {
		t.Error("block position does not get overridden")
	}
}

func TestAddItemToIndexWithInsuffiecientKeys(t *testing.T) {
	index := NewMemIndex([]string{"appId", "host"})

	item := map[string]string{"appId": "kadira"}
	var blockPosition int64 = 10
	err := index.AddItem(item, blockPosition)

	if err == nil {
		t.Error("need to have an error", err)
	}
}

func TestAddItemToIndexWithNoKeys(t *testing.T) {
	index := NewMemIndex([]string{"appId", "host"})

	item := map[string]string{}
	var blockPosition int64 = 10
	err := index.AddItem(item, blockPosition)

	if err == nil {
		t.Error("need to have an error", err)
	}
}

// GetElement

func TestGetElement(t *testing.T) {
	index := NewMemIndex([]string{"appId", "host"})

	item := map[string]string{"appId": "kadira", "host": "h1"}
	var blockPosition int64 = 10
	index.AddItem(item, blockPosition)

	_, addedItem := index.GetElement(item)

	if addedItem == nil {
		t.Error("couldn't get the added item")
	}

	if addedItem.BlockPosition != blockPosition {
		t.Error("block position must be present")
	}

	expectedValues := []string{"kadira", "h1"}
	if !reflect.DeepEqual(addedItem.Values, expectedValues) {
		t.Error("values are incorrect")
	}
}

func TestGetElementNoSuchElement(t *testing.T) {
	index := NewMemIndex([]string{"appId", "host"})

	item := map[string]string{"appId": "kadira", "host": "h1"}
	var blockPosition int64 = 10
	index.AddItem(item, blockPosition)

	nonExistingEl := map[string]string{"appId": "kadira", "host": "h2"}
	err, addedItem := index.GetElement(nonExistingEl)

	if err != nil {
		t.Error("cannot have a error")
	}

	if addedItem != nil {
		t.Error("cannot have element")
	}
}

func TestGetElementInsufficientKeys(t *testing.T) {
	index := NewMemIndex([]string{"appId", "host"})

	item := map[string]string{"appId": "kadira", "host": "h1"}
	var blockPosition int64 = 10
	index.AddItem(item, blockPosition)

	query := map[string]string{"appId": "kadira"}
	err, _ := index.GetElement(query)

	if err == nil {
		t.Error("need to have an error")
	}
}

// Find Elements

func TestFindElementsInBottom(t *testing.T) {
	index := NewMemIndex([]string{"appId", "host"})

	item := map[string]string{"appId": "kadira", "host": "h1"}
	var blockPosition int64 = 10
	index.AddItem(item, blockPosition)

	_, elements := index.FindElements(item)

	if len(elements) != 1 {
		t.Error("could not find the element")
	}

	if elements[0].BlockPosition != blockPosition {
		t.Error("found the wrong element")
	}
}

func TestFindElementsInMiddleRoot(t *testing.T) {
	index := NewMemIndex([]string{"appId", "host"})
	index.AddItem(map[string]string{"appId": "kadira", "host": "h1"}, 10)
	index.AddItem(map[string]string{"appId": "kadira", "host": "h2"}, 20)
	index.AddItem(map[string]string{"appId": "some-other", "host": "h2"}, 20)

	_, elements := index.FindElements(map[string]string{"appId": "kadira"})

	if len(elements) != 2 {
		t.Error("could not find the element")
	}

	blockPositions := make([]int64, len(elements))
	for lc, el := range elements {
		blockPositions[lc] = el.BlockPosition
	}

	if !reflect.DeepEqual(blockPositions, []int64{10, 20}) {
		t.Error("found wrong elements")
	}
}

func TestFindElementsInTopLevel(t *testing.T) {
	index := NewMemIndex([]string{"appId", "host"})
	index.AddItem(map[string]string{"appId": "kadira", "host": "h1"}, 10)
	index.AddItem(map[string]string{"appId": "kadira", "host": "h2"}, 20)
	index.AddItem(map[string]string{"appId": "some-other", "host": "h2"}, 30)
	index.AddItem(map[string]string{"appId": "coolio", "host": "h2"}, 40)

	_, elements := index.FindElements(map[string]string{})

	if len(elements) != 4 {
		t.Error("could not find the element")
	}

	blockPositions := make([]int, len(elements))
	for lc, el := range elements {
		blockPositions[lc] = int(el.BlockPosition)
	}

	sort.Ints(blockPositions)

	if !reflect.DeepEqual(blockPositions, []int{10, 20, 30, 40}) {
		t.Error("found wrong elements", blockPositions)
	}
}
