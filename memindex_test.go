package kdb

import (
	"reflect"
	"sort"
	"testing"
)

// Create Index

func TestCreateIndex(t *testing.T) {
	keys := []string{"appId", "host"}
	index := NewMemIndex(keys)
	if !reflect.DeepEqual(index.Keys, keys) {
		t.Fatal("orderedKey must be set")
	}

	if index.Root == nil {
		t.Fatal("index root must be set")
	}
}

// Add to Index

func TestAddItemToIndex(t *testing.T) {
	index := NewMemIndex([]string{"appId", "host"})
	item := map[string]string{"appId": "kadira", "host": "h1"}

	var position int64 = 10
	index.AddItem(item, position)

	addedItem := index.Root.Children["kadira"].Children["h1"]

	if addedItem.Position != position {
		t.Fatal("block position must be present")
	}

	expected := []string{"kadira", "h1"}
	if !reflect.DeepEqual(addedItem.Values, expected) {
		t.Fatal("values are incorrect")
	}
}

func TestAddItemToIndexOverride(t *testing.T) {
	index := NewMemIndex([]string{"appId", "host"})
	item := map[string]string{"appId": "kadira", "host": "h1"}

	var position int64 = 10
	index.AddItem(item, position)

	position = 100
	index.AddItem(item, position)

	addedItem := index.Root.Children["kadira"].Children["h1"]

	if addedItem.Position != position {
		t.Fatal("block position does not get overridden")
	}
}

func TestAddItemToIndexWithInsuffiecientKeys(t *testing.T) {
	index := NewMemIndex([]string{"appId", "host"})
	item := map[string]string{"appId": "kadira"}

	var position int64 = 10
	err := index.AddItem(item, position)

	if err == nil {
		t.Fatal("need to have an error", err)
	}
}

func TestAddItemToIndexWithNoKeys(t *testing.T) {
	index := NewMemIndex([]string{"appId", "host"})
	item := map[string]string{}

	var position int64 = 10
	err := index.AddItem(item, position)

	if err == nil {
		t.Fatal("need to have an error", err)
	}
}

// GetElement

func TestGetElement(t *testing.T) {
	index := NewMemIndex([]string{"appId", "host"})
	item := map[string]string{"appId": "kadira", "host": "h1"}

	var position int64 = 10
	index.AddItem(item, position)

	addedItem, _ := index.GetElement(item)

	if addedItem == nil {
		t.Fatal("couldn't get the added item")
	}

	if addedItem.Position != position {
		t.Fatal("block position must be present")
	}

	expectedValues := []string{"kadira", "h1"}
	if !reflect.DeepEqual(addedItem.Values, expectedValues) {
		t.Fatal("values are incorrect")
	}
}

func TestGetElementNoSuchElement(t *testing.T) {
	index := NewMemIndex([]string{"appId", "host"})
	item := map[string]string{"appId": "kadira", "host": "h1"}

	var position int64 = 10
	index.AddItem(item, position)

	nonExistingEl := map[string]string{"appId": "kadira", "host": "h2"}
	addedItem, err := index.GetElement(nonExistingEl)

	if err != nil {
		t.Fatal("cannot have a error")
	}

	if addedItem != nil {
		t.Fatal("cannot have element")
	}
}

func TestGetElementInsufficientKeys(t *testing.T) {
	index := NewMemIndex([]string{"appId", "host"})
	item := map[string]string{"appId": "kadira", "host": "h1"}

	var position int64 = 10
	index.AddItem(item, position)

	query := map[string]string{"appId": "kadira"}
	_, err := index.GetElement(query)

	if err == nil {
		t.Fatal("need to have an error")
	}
}

// Find Elements

func TestFindElementsInBottom(t *testing.T) {
	index := NewMemIndex([]string{"appId", "host"})
	item := map[string]string{"appId": "kadira", "host": "h1"}

	var position int64 = 10
	index.AddItem(item, position)

	elements, _ := index.FindElements(item)

	if len(elements) != 1 {
		t.Fatal("could not find the element")
	}

	if elements[0].Position != position {
		t.Fatal("found the wrong element")
	}
}

func TestFindElementsInMiddleRoot(t *testing.T) {
	index := NewMemIndex([]string{"appId", "host"})
	index.AddItem(map[string]string{"appId": "kadira", "host": "h1"}, 10)
	index.AddItem(map[string]string{"appId": "kadira", "host": "h2"}, 20)
	index.AddItem(map[string]string{"appId": "some-other", "host": "h2"}, 20)

	elements, _ := index.FindElements(map[string]string{"appId": "kadira"})

	if len(elements) != 2 {
		t.Fatal("could not find the element")
	}

	positions := make([]int64, len(elements))
	for lc, el := range elements {
		positions[lc] = el.Position
	}

	if !reflect.DeepEqual(positions, []int64{10, 20}) {
		t.Fatal("found wrong elements")
	}
}

func TestFindElementsInTopLevel(t *testing.T) {
	index := NewMemIndex([]string{"appId", "host"})
	index.AddItem(map[string]string{"appId": "kadira", "host": "h1"}, 10)
	index.AddItem(map[string]string{"appId": "kadira", "host": "h2"}, 20)
	index.AddItem(map[string]string{"appId": "some-other", "host": "h2"}, 30)
	index.AddItem(map[string]string{"appId": "coolio", "host": "h2"}, 40)

	elements, _ := index.FindElements(map[string]string{})

	if len(elements) != 4 {
		t.Fatal("could not find the element")
	}

	positions := make([]int, len(elements))
	for lc, el := range elements {
		positions[lc] = int(el.Position)
	}

	sort.Ints(positions)

	if !reflect.DeepEqual(positions, []int{10, 20, 30, 40}) {
		t.Fatal("found wrong elements", positions)
	}
}
