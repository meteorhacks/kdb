//use go generate to build protocol buffer source files
//go:generate protoc --proto_path=$GOPATH/src:$GOPATH/src/github.com/gogo/protobuf/protobuf/:. --gogo_out=. mindex.proto

package mindex

import (
	"encoding/binary"
	"errors"
	"os"
	"runtime"
	"sync"
	"syscall"

	"github.com/meteorhacks/kdb"
)

const (
	MIndexFMode        = os.O_CREATE | os.O_RDWR
	MIndexFPerms       = 0644
	MIndexElHeaderSize = 8
	MIndexPaddingSize  = 4
	PreAllocateSize    = 1024 * 1024 * 10 // 10 Mb pre allocation
)

var (
	MIndexPadding                 = []byte{0, 0, 0, 0}
	ErrMIndexBytesWrittenToFile   = errors.New("incorrect number of bytes written to index file")
	ErrMIndexBytesWrittenToBuffer = errors.New("incorrect number of bytes written to temporary buffer")
	ErrMIndexBytesReadFromFile    = errors.New("incorrect number of bytes read from index file")
	ErrMIndexBytesReadFromBuffer  = errors.New("incorrect number of bytes read from temporary buffer")
)

type MIndexOpts struct {
	// path to block file
	FilePath string

	// depth of the index tree
	IndexDepth int64

	// partition number
	PartitionNo int64
}

// Base struct of the MIndex
// `root` is the starting point of the tree
type MIndex struct {
	MIndexOpts
	root            *kdb.IndexElement // root element of the index tree
	file            *os.File          // file used to store index nodes
	currentFileSize int64             // file size (offset to place next index)
	totalFileSize   int64             // total file of the file
	mmapedData      []byte            // mmaped file data
	mmapedOffset    int64             // offset of the mmap
	mutex           *sync.Mutex
}

func NewMIndex(opts MIndexOpts) (idx *MIndex, err error) {
	file, err := os.OpenFile(opts.FilePath, MIndexFMode, MIndexFPerms)
	if err != nil {
		// A not found error will be thrown here if the bucket which
		// is creating this index does not exist in the filesystem.
		// This is currently used to identify whether the bucket is
		// available in the disk (which is a terrible thing to do).
		// TODO: use a better way to find buckets not in the disk.
		return nil, err
	}

	root := &kdb.IndexElement{
		Children: make(map[string]*kdb.IndexElement),
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

	idx = &MIndex{opts, root, file, currentFileSize, totalFileSize, mmapedData, mmapedOffset, mutex}

	if err := idx.load(); err != nil {
		return nil, err
	}

	return idx, nil
}

// Add Item to the index with provided record position
func (idx *MIndex) Add(vals []string, rpos int64) (el *kdb.IndexElement, err error) {
	el = &kdb.IndexElement{
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
func (idx *MIndex) Get(vals []string) (el *kdb.IndexElement, err error) {
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
func (idx *MIndex) Find(vals []string) (els []*kdb.IndexElement, err error) {
	els = make([]*kdb.IndexElement, 0)
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
func (idx *MIndex) Close() (err error) {
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
func (idx *MIndex) load() (err error) {
	idxEl := MIndexEl{}

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
		sizeData := data[offset : offset+MIndexElHeaderSize]

		idxElSize, n := binary.Varint(sizeData)

		if n <= 0 {
			return ErrMIndexBytesReadFromBuffer
		}

		// read encoded element and Unmarshal it
		start := offset + MIndexElHeaderSize
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
		offset += MIndexElHeaderSize + idxElSize + MIndexPaddingSize

		el := &kdb.IndexElement{
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
func (idx *MIndex) find(root *kdb.IndexElement, els []*kdb.IndexElement) []*kdb.IndexElement {
	if root.Children == nil {
		return append(els, root)
	}

	for _, el := range root.Children {
		els = idx.find(el, els)
	}

	return els
}

// add IndexElement to the tree
func (idx *MIndex) addElement(el *kdb.IndexElement) (err error) {
	root := idx.root
	tempVals := make([]string, 4)

	for i, v := range el.Values[0 : idx.IndexDepth-1] {
		newRoot, ok := root.Children[v]
		tempVals[i] = v

		if !ok {
			newRoot = &kdb.IndexElement{}
			newRoot.Children = make(map[string]*kdb.IndexElement)
			root.Children[v] = newRoot
		}

		root = newRoot
	}

	lastValue := el.Values[idx.IndexDepth-1]
	root.Children[lastValue] = el

	return nil
}

// Element is saved in format [size element padding]
func (idx *MIndex) saveElement(el *kdb.IndexElement) (err error) {
	mel := MIndexEl{
		Position: &el.Position,
		Values:   el.Values,
	}

	elementSize := mel.Size()
	totalSize := int64(MIndexElHeaderSize + elementSize + MIndexPaddingSize)
	data := make([]byte, totalSize, totalSize)

	// add the element header (int64 of element size)
	binary.PutVarint(data, int64(elementSize))

	// add the protobuffer encoded element to the payload
	elData := data[MIndexElHeaderSize : MIndexElHeaderSize+elementSize]
	n, err := mel.MarshalTo(elData)
	if err != nil {
		return err
	} else if n != elementSize {
		return ErrMIndexBytesWrittenToBuffer
	}

	// add the padding at the end of the payload
	copy(data[MIndexElHeaderSize+elementSize:], MIndexPadding)

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

func (idx *MIndex) preAllocateIfNeeded(sizeNeedToWrite int64) (err error) {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	excessBytes := idx.totalFileSize - idx.currentFileSize
	if excessBytes <= sizeNeedToWrite {

		// let's allocate some bytes
		allocateAmount := PreAllocateSize - excessBytes
		emptyBytes := make([]byte, allocateAmount)

		// let's unmap the previous mapped data
		syscall.Munmap(idx.mmapedData)

		_, err := idx.file.WriteAt(emptyBytes, idx.currentFileSize)
		if err != nil {
			return err
		}

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

func (idx *MIndex) loadData(start, end int64) (err error) {
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
