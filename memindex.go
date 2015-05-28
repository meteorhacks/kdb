package kdb

import (
	"encoding/binary"
	"errors"
	"os"
	"runtime"
	"sync"
	"syscall"
)

const (
	MemIndexFMode        = os.O_CREATE | os.O_RDWR
	MemIndexFPerms       = 0644
	MemIndexElHeaderSize = 8
	MemIndexPaddingSize  = 4
	PreAllocateSize      = 1024 * 1024 * 10 // 10 Mb pre allocation
)

var (
	MemIndexPadding                 = []byte{0, 0, 0, 0}
	ErrMemIndexBytesWrittenToFile   = errors.New("incorrect number of bytes written to index file")
	ErrMemIndexBytesWrittenToBuffer = errors.New("incorrect number of bytes written to temporary buffer")
	ErrMemIndexBytesReadFromFile    = errors.New("incorrect number of bytes read from index file")
	ErrMemIndexBytesReadFromBuffer  = errors.New("incorrect number of bytes read from temporary buffer")
)

type MemIndexOpts struct {
	// path to block file
	FilePath string

	// depth of the index tree
	IndexDepth int64

	// partition number
	PartitionNo int64
}

// Base struct of the MemIndex
// `root` is the starting point of the tree
type MemIndex struct {
	MemIndexOpts
	root            *IndexElement // root element of the index tree
	file            *os.File      // file used to store index nodes
	currentFileSize int64         // file size (offset to place next index)
	totalFileSize   int64         // total file of the file
	mmapedData      []byte        // mmaped file data
	mmapedOffset    int64         // offset of the mmap
	mutex           *sync.Mutex
}

func NewMemIndex(opts MemIndexOpts) (idx *MemIndex, err error) {
	file, err := os.OpenFile(opts.FilePath, MemIndexFMode, MemIndexFPerms)
	if err != nil {
		return nil, err
	}

	root := &IndexElement{
		Children: make(map[string]*IndexElement),
	}

	finfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	currentFileSize := finfo.Size()
	totalFileSize := finfo.Size()
	mmapedOffset := int64(0)
	mmapedData := make([]byte, 0)

	mutex := &sync.Mutex{}

	idx = &MemIndex{opts, root, file, currentFileSize, totalFileSize, mmapedData, mmapedOffset, mutex}

	if err := idx.load(); err != nil {
		return nil, err
	}

	return idx, nil
}

// Add Item to the index with provided record position
func (idx *MemIndex) Add(vals []string, rpos int64) (el *IndexElement, err error) {
	el = &IndexElement{
		Position: rpos,
		Values:   vals,
	}

	err = idx.addElement(el)
	if err != nil {
		return nil, err
	}

	err = idx.saveElement(el)
	if err != nil {
		return nil, err
	}

	return el, nil
}

// Get the IndexElement for given set of values
func (idx *MemIndex) Get(vals []string) (el *IndexElement, err error) {
	el = idx.root
	var ok bool

	for _, v := range vals {
		if el, ok = el.Children[v]; !ok {
			return nil, nil
		}
	}

	return el, nil
}

// Get the IndexElement for given set of values
func (idx *MemIndex) Find(vals []string) (els []*IndexElement, err error) {
	els = make([]*IndexElement, 0)
	root := idx.root
	var ok bool

	needsFilter := false

	for j, v := range vals {
		if v == "" {
			for i := len(vals) - 1; i >= j; i-- {
				if vals[i] != "" {
					needsFilter = true
				}
			}

			break
		}

		if root, ok = root.Children[v]; !ok {
			return els, nil
		}
	}

	els = idx.find(root, els)
	if !needsFilter {
		return els, nil
	}

	filtered := els[:0]

outer:
	for _, el := range els {
		for j, _ := range vals {
			if vals[j] != "" && vals[j] != el.Values[j] {
				continue outer
			}
		}

		filtered = append(filtered, el)
	}

	return filtered, nil
}

// close the file handler
func (idx *MemIndex) Close() (err error) {
	err = idx.file.Close()
	if err != nil {
		return err
	}

	err = syscall.Munmap(idx.mmapedData)
	if err != nil {
		return err
	}

	return nil
}

// loads index data from a file containing protobuf encoded index elements
// TODO: handle corrupt index files (load valid index points)
func (idx *MemIndex) load() (err error) {
	idxEl := MemIndexElement{}

	err = idx.loadData(0, idx.totalFileSize)
	if err != nil {
		return err
	}

	data := idx.mmapedData
	dataSize := int64(len(data))
	var offset int64 = 0

	// decode index elements one by one from the index file
	for {
		if offset == dataSize {
			break
		}

		// reset `idxEl` struct values
		// reuse to avoid memory allocations
		idxEl.Position = nil
		idxEl.Values = nil

		// read element header (element size as int64) from data
		sizeData := data[offset : offset+MemIndexElHeaderSize]

		idxElSize, n := binary.Varint(sizeData)

		if n <= 0 {
			return ErrMemIndexBytesReadFromBuffer
		}

		// read encoded element and Unmarshal it
		start := offset + MemIndexElHeaderSize
		end := start + idxElSize
		if end > idx.totalFileSize {
			return errors.New("data size is too small to filled into protobuf")
		}

		idxElData := data[start:end]

		err = idxEl.Unmarshal(idxElData)
		// we've reached to the end of the data
		if err != nil {
			break
		}

		// set offset to point to the end of bytes already read
		offset += MemIndexElHeaderSize + idxElSize + MemIndexPaddingSize

		el := &IndexElement{
			Position: *idxEl.Position,
			Values:   idxEl.Values,
		}

		if err = idx.addElement(el); err != nil {
			return err
		}
	}

	idx.currentFileSize = offset

	err = idx.preAllocateIfNeeded(0)
	if err != nil {
		return err
	}

	return nil
}

// recursively go through all tree branches and collect leaf nodes
func (idx *MemIndex) find(root *IndexElement, els []*IndexElement) []*IndexElement {
	if root.Children == nil {
		return append(els, root)
	}

	for _, el := range root.Children {
		els = idx.find(el, els)
	}

	return els
}

// add IndexElement to the tree
func (idx *MemIndex) addElement(el *IndexElement) (err error) {
	root := idx.root
	tempVals := make([]string, 4)

	for i, v := range el.Values[0 : idx.IndexDepth-1] {
		newRoot, ok := root.Children[v]
		tempVals[i] = v

		if !ok {
			newRoot = &IndexElement{}
			newRoot.Children = make(map[string]*IndexElement)
			root.Children[v] = newRoot
		}

		root = newRoot
	}

	lastValue := el.Values[idx.IndexDepth-1]
	root.Children[lastValue] = el

	return nil
}

// Element is saved in format [size element padding]
func (idx *MemIndex) saveElement(el *IndexElement) (err error) {
	mel := MemIndexElement{
		Position: &el.Position,
		Values:   el.Values,
	}

	elementSize := mel.Size()
	totalSize := int64(MemIndexElHeaderSize + elementSize + MemIndexPaddingSize)
	data := make([]byte, totalSize, totalSize)

	// add the element header (int64 of element size)
	binary.PutVarint(data, int64(elementSize))

	// add the protobuffer encoded element to the payload
	elData := data[MemIndexElHeaderSize : MemIndexElHeaderSize+elementSize]
	n, err := mel.MarshalTo(elData)
	if err != nil {
		return err
	} else if n != elementSize {
		return ErrMemIndexBytesWrittenToBuffer
	}

	// add the padding at the end of the payload
	copy(data[MemIndexElHeaderSize+elementSize:], MemIndexPadding)

	idx.preAllocateIfNeeded(totalSize)

	// finally, write the payload to the file
	offset := idx.currentFileSize
	idx.mutex.Lock()

	var lc int64 = 0
	for lc = 0; lc < int64(len(data)); lc++ {
		// We may read from a different offset and didn't start from the begining
		// In that case, we need to reduce the mmap starting point from the offset
		pos := offset + lc - idx.mmapedOffset
		idx.mmapedData[pos] = data[lc]
	}

	idx.currentFileSize += totalSize
	idx.mutex.Unlock()
	runtime.Gosched()

	return nil
}

func (idx *MemIndex) preAllocateIfNeeded(sizeNeedToWrite int64) (err error) {
	excessBytes := idx.totalFileSize - idx.currentFileSize
	if excessBytes <= sizeNeedToWrite {
		// let's allocate some bytes
		allocateAmount := PreAllocateSize - excessBytes
		emptyBytes := make([]byte, allocateAmount)

		idx.mutex.Lock()
		// let's unmap the previous mapped data
		syscall.Munmap(idx.mmapedData)

		_, err := idx.file.WriteAt(emptyBytes, idx.currentFileSize)
		if err != nil {
			return err
		}
		idx.mutex.Unlock()

		// let's allocate again
		idx.totalFileSize += allocateAmount

		// TODO: right now we need to start from 0 to read even we are appending
		// reading from random places works well in OSX. But on linux, we need
		// to set on the offset into a multiple of page size
		// We can implement it, but it's pretty okay to start allocating
		// from the beginning
		idx.loadData(0, idx.totalFileSize)
	}

	return nil
}

func (idx *MemIndex) loadData(start, end int64) (err error) {
	fd := int(idx.file.Fd())
	length := end - start
	prot := syscall.PROT_READ | syscall.PROT_WRITE
	flags := syscall.MAP_SHARED

	// if there is no data, we can't mmap
	if length == 0 {
		idx.mmapedData = make([]byte, 0)
		idx.mmapedOffset = 0
		return nil
	}

	// mmap the data
	data, err := syscall.Mmap(fd, start, int(length), prot, flags)
	if err != nil {
		return err
	}

	idx.mmapedData = data
	idx.mmapedOffset = start
	return nil
}
