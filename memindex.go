package kdb

import (
	"fmt"
)

// Struct representing an element in the index. Here we are maintaining a
// tree structure. So, it's `Values` field only containes in the leaf nodes only
// `Map` only conatains in root and intermediate nodes only
//
// Here all the data elements are on the lowest level, which are leafs
type IndexElement struct {
	Values        []string
	BlockPosition int64
	Map           map[string]*IndexElement
}

// Base struct of the MemIndex
// It contains an element called `root` which is the starting point of the tree
// orderedKeys contains all the keys in this index in ordered fashion
type MemIndex struct {
	root        *IndexElement
	orderedKeys []string
}

// Create a new MemIndex for a given set of keys
// All the items we add needs to have all the keys defined here
// Here's an example:
//
//
// 	index := NewMemIndex(string[]{"appId", "metric", "host"})
// 	var blockPosition int64 = 1000;
// 	item := map[string]string{"appId": "kadira", "metric": "cpu", "host": "h1"}
// 	err := index.addItem(item, blockPosition)
// 	fmt.Println(err)

// 	err, element := index.getElement(item)
// 	fmt.Println("blockPosition is:", element.BlockPosition)
//

func NewMemIndex(orderedKeys []string) MemIndex {
	index := MemIndex{}
	index.root = makeIndexElement()
	index.orderedKeys = orderedKeys

	return index
}

// Add Item to the index with the blockPosition
// return an error, if the item does not have all the keys needs by the index
func (m MemIndex) AddItem(item map[string]string, blockPosition int64) error {
	immediateRoot := m.root
	lenKeys := len(m.orderedKeys)

	el := IndexElement{}
	el.Values = make([]string, lenKeys)
	el.BlockPosition = blockPosition

	for lc, key := range m.orderedKeys[0 : lenKeys-1] {
		value := item[key]

		if value == "" {
			return fmt.Errorf("no value for `%s`", key)
		}

		newImmediateRoot := immediateRoot.Map[value]
		if newImmediateRoot == nil {
			newImmediateRoot = makeIndexElement()
			immediateRoot.Map[value] = newImmediateRoot
		}

		immediateRoot = newImmediateRoot
		el.Values[lc] = value
	}

	lastKey := m.orderedKeys[lenKeys-1 : lenKeys][0]
	lastValue := item[lastKey]

	if lastValue == "" {
		return fmt.Errorf("no value for `%s`", lastKey)
	}

	el.Values[lenKeys-1] = lastValue

	immediateRoot.Map[lastValue] = &el

	return nil
}

// Get the IndexElement related to item
// With that we can get the BlockPosition we've added
// Return an error if item does not have all the keys in the index
func (m MemIndex) GetElement(item map[string]string) (error, *IndexElement) {
	values := make([]string, len(m.orderedKeys))

	for lc, key := range m.orderedKeys {
		value := item[key]
		if value == "" {
			return fmt.Errorf("no value for '%s'", key), nil
		}

		values[lc] = value
	}

	el := m.root
	for _, value := range values {
		el = el.Map[value]
		if el == nil {
			return nil, nil
		}
	}

	return nil, el
}

// HighLevel function to find elements in the index
// If we've an index with keys `appId`, `metric`, `host`, we can query for
// subset of index.
// for an example, we can query for
//	* all the items of a given appId
//	* all the cpu meterics of a given appId
//	* all the cpu meterics of a given appId and host
//
//	 _, elements := index.FindElement(map[string]string{"appId": "abc", "metric": "cpu"})
//
func (m MemIndex) FindElements(query map[string]string) (error, []*IndexElement) {
	values := make([]string, 0)
	elements := make([]*IndexElement, 0)

	for _, key := range m.orderedKeys {
		value := query[key]
		if value == "" {
			break
		}

		values = append(values, value)
	}

	el := m.root
	for _, value := range values {
		el = el.Map[value]
		if el == nil {
			return nil, elements
		}
	}

	return nil, findElements(el, elements)
}

func findElements(el *IndexElement, elements []*IndexElement) []*IndexElement {
	isRootNode := el.Map != nil
	if isRootNode {
		for _, value := range el.Map {
			elements = findElements(value, elements)
		}
		return elements
	} else {
		return append(elements, el)
	}
}

func makeIndexElement() *IndexElement {
	el := IndexElement{}
	el.Map = make(map[string]*IndexElement)
	return &el
}
