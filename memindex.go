package kdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
)

const (
	MemIndexFMode  = os.O_CREATE | os.O_RDWR
	MemIndexFPerms = 0644
)

var (
	ErrMemIndexIncorrectDepth = errors.New("incorrect number of values")
	ErrMemIndexBytesWritten   = errors.New("incorrect number of bytes written to index file")
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
	root  *IndexElement // root element of the index tree
	file  *os.File      // file used to store index nodes
	fsize int64         // file size (offset to place next index)
	buff  *bytes.Buffer // to temporarily store json data
	mutex *sync.Mutex
}

func NewMemIndex(opts MemIndexOpts) (idx *MemIndex, err error) {
	log.Print("MemIndex: loading index file for partition: ", opts.PartitionNo)

	file, err := os.OpenFile(opts.FilePath, MemIndexFMode, MemIndexFPerms)
	if err != nil {
		log.Print("MemIndex: could not open memindex file: ", opts.FilePath)
		return nil, err
	}

	finfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	fsize := finfo.Size()
	decd := json.NewDecoder(file)
	mutex := &sync.Mutex{}
	buff := new(bytes.Buffer)

	idx = &MemIndex{opts, nil, file, fsize, buff, mutex}

	idx.root = &IndexElement{}
	idx.root.Children = make(map[string]*IndexElement)

	// decode index elements one by one from the index file
	// index file has json encoded
	for {
		el := &IndexElement{}

		if err = decd.Decode(el); err == io.EOF {
			break
		} else if err != nil {
			log.Print("MemIndex: error while decoding index value from file", opts.FilePath)
			return nil, err
		}

		if err = idx.add(el); err != nil {
			return nil, err
		}
	}

	return idx, nil
}

func (idx *MemIndex) NewIndexElement(vals []string) (el *IndexElement, err error) {
	el = &IndexElement{}
	el.Children = make(map[string]*IndexElement)
	el.Values = vals

	idx.buff.Reset()
	encd := json.NewEncoder(idx.buff)

	err = encd.Encode(el)
	if err != nil {
		return nil, err
	}

	data := idx.buff.Bytes()
	elSize := len(data)

	idx.mutex.Lock()
	offset := idx.fsize

	n, err := idx.file.WriteAt(data, offset)
	if err == nil && n != elSize {
		err = ErrMemIndexBytesWritten
	}

	if err == nil {
		idx.fsize += int64(elSize)
	}

	idx.mutex.Unlock()
	runtime.Gosched()

	if err != nil {
		return nil, err
	}

	return el, nil
}

// Add Item to the index with provided record position
func (idx *MemIndex) Add(vals []string, rpos int64) (el *IndexElement, err error) {
	el, err = idx.NewIndexElement(vals)
	if err != nil {
		return nil, err
	}

	el.Values = vals
	el.Position = rpos

	err = idx.add(el)
	if err != nil {
		return nil, err
	}

	return el, nil
}

// add IndexElement
func (idx *MemIndex) add(el *IndexElement) (err error) {
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

	for _, v := range vals {
		if v == "" {
			break
		}

		if root, ok = root.Children[v]; !ok {
			return els, nil
		}
	}

	els = idx.find(root, els)

	return els, nil
}

// recursively go through all tree branches and collect leaf nodes
func (idx *MemIndex) find(root *IndexElement, els []*IndexElement) []*IndexElement {
	if len(root.Children) == 0 {
		return append(els, root)
	}

	for _, el := range root.Children {
		els = idx.find(el, els)
	}

	return els
}

// close the file handler
func (idx *MemIndex) Close() (err error) {
	err = idx.file.Close()
	if err != nil {
		return err
	}

	return nil
}
