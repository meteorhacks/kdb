package kdb

import "errors"

// Struct representing an element in the index. Here we are maintaining a
// tree structure. So, it's `Values` field only containes in the leaf nodes only
// `Children` only conatains in root and intermediate nodes only
//
// Here all the data elements are on the lowest level, which are leafs
type IndexElement struct {
	Values   []string
	Position int64
	Children map[string]*IndexElement
}

// Base struct of the MemIndex
// It contains an element called `root` which is the starting point of the tree
// keys contains all the keys in this index in ordered fashion
type MemIndex struct {
	root *IndexElement
	keys []string
}

// Create a new MemIndex for a given set of keys
// All the items we add needs to have all the keys defined here
// Here's an example:
//
//
// 	index := NewMemIndex(string[]{"appId", "metric", "host"})
// 	var position int64 = 1000;
// 	item := map[string]string{"appId": "kadira", "metric": "cpu", "host": "h1"}
// 	err := index.addItem(item, position)
// 	fmt.Println(err)

// 	err, element := index.getElement(item)
// 	fmt.Println("position is:", element.Position)
//

func NewMemIndex(keys []string) (mi *MemIndex) {
	mi = &MemIndex{nil, keys}
	mi.root = mi.newElement()

	return mi
}

// TODO: add comment
func (mi *MemIndex) newElement() (el *IndexElement) {
	el = &IndexElement{}
	el.Children = make(map[string]*IndexElement)
	return el
}

// Add Item to the index with the position
// return an error, if the item does not have all the keys needs by the index
func (mi *MemIndex) AddItem(item map[string]string, position int64) (err error) {
	root := mi.root
	lenKeys := len(mi.keys)

	el := IndexElement{}
	el.Values = make([]string, lenKeys)
	el.Position = position

	for lc, key := range mi.keys[0 : lenKeys-1] {
		value := item[key]

		if value == "" {
			err = errors.New("no value for " + key)
			return err
		}

		newRoot := root.Children[value]
		if newRoot == nil {
			newRoot = mi.newElement()
			root.Children[value] = newRoot
		}

		root = newRoot
		el.Values[lc] = value
	}

	lastKey := mi.keys[lenKeys-1 : lenKeys][0]
	lastValue := item[lastKey]

	if lastValue == "" {
		err = errors.New("no value for " + lastKey)
		return err
	}

	el.Values[lenKeys-1] = lastValue

	root.Children[lastValue] = &el

	return nil
}

// Get the IndexElement related to item
// With that we can get the Position we've added
// Return an error if item does not have all the keys in the index
func (mi *MemIndex) GetElement(item map[string]string) (el *IndexElement, err error) {
	values := make([]string, len(mi.keys))

	for lc, key := range mi.keys {
		value := item[key]
		if value == "" {
			err = errors.New("no value for " + key)
			return nil, err
		}

		values[lc] = value
	}

	el = mi.root
	for _, value := range values {
		el = el.Children[value]
		if el == nil {
			return nil, nil
		}
	}

	return el, nil
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
func (mi *MemIndex) FindElements(query map[string]string) (els []*IndexElement, err error) {
	values := make([]string, 0)
	els = make([]*IndexElement, 0)

	for _, key := range mi.keys {
		value := query[key]
		if value == "" {
			break
		}

		values = append(values, value)
	}

	el := mi.root
	for _, value := range values {
		el = el.Children[value]
		if el == nil {
			return els, nil
		}
	}

	els = mi.findElements(el, els)

	return els, nil
}

// TODO: add comment
func (mi *MemIndex) findElements(el *IndexElement, els []*IndexElement) []*IndexElement {
	if el.Children != nil {
		for _, value := range el.Children {
			els = mi.findElements(value, els)
		}

		return els
	} else {
		return append(els, el)
	}
}
