package kdb

type IndexElement struct {
	Values        []string
	BlockPosition int64
	Map           map[string]*IndexElement
}

type MemIndex struct {
	root        *IndexElement
	orderedKeys []string
}

func (m MemIndex) AddItem(element map[string]string, blockPosition int64) {
	immediateRoot := m.root
	lenKeys := len(m.orderedKeys)

	el := IndexElement{}
	el.Values = make([]string, lenKeys)
	el.BlockPosition = blockPosition

	for lc := 0; lc < lenKeys-1; lc++ {
		key := m.orderedKeys[lc]
		value := element[key]
		newImmediateRoot := immediateRoot.Map[value]
		if newImmediateRoot == nil {
			newImmediateRoot = makeIndexElement()
			immediateRoot.Map[value] = newImmediateRoot
		}

		immediateRoot = newImmediateRoot
		el.Values[lc] = value
	}

	lastKey := m.orderedKeys[lenKeys-1 : lenKeys][0]
	lastValue := element[lastKey]
	el.Values[lenKeys-1] = lastValue

	immediateRoot.Map[lastValue] = &el
}

func NewMemIndex(orderedKeys []string) MemIndex {
	index := MemIndex{}
	index.root = makeIndexElement()
	index.orderedKeys = orderedKeys

	return index
}

func makeIndexElement() *IndexElement {
	el := IndexElement{}
	el.Map = make(map[string]*IndexElement)
	return &el
}
