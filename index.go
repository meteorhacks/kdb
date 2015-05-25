package kdb

import (
	"encoding/json"
	"os"
)

type IndexOpts struct {
	Keys []string // index keys
	Path string   // path to the index file
}

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

type Index struct {
	opts IndexOpts
	data *MemIndex     // in-memory index
	file *os.File      // index file handler
	encd *json.Encoder // use protobuff for better perf
	decd *json.Decoder // use protobuff for better perf
}

func NewIndex(opts IndexOpts) (in *Index, err error) {
	fd, err := os.OpenFile(opts.Path, FMode, FPerms)
	if err != nil {
		return nil, err
	}

	mi := NewMemIndex(opts.Keys)
	en := json.NewEncoder(fd)
	de := json.NewDecoder(fd)
	in = &Index{opts, mi, fd, en, de}

	return in, nil
}

func (in *Index) Get(item map[string]string) (el *IndexElement, err error) {
	return in.data.GetElement(item)
}

func (in *Index) Find(query map[string]string) (els []*IndexElement, err error) {
	return in.data.FindElements(query)
}

func (in *Index) MakeQuery(el *IndexElement) (query map[string]string, err error) {
	return in.data.MakeQuery(el)
}

func (in *Index) AddItem(item map[string]string, position int64) (el *IndexElement, err error) {
	return in.data.AddItem(item, position)
}

// writes the index map to the filesystem
// TODO: try to avoid writing the whole map
func (in *Index) Sync() (err error) {
	if err = in.seekToStart(); err != nil {
		return err
	}

	if err = in.encd.Encode(in.data); err != nil {
		return err
	}

	return nil
}

// Load function builds the index map from the file
func (in *Index) Load() (err error) {
	if err = in.seekToStart(); err != nil {
		return err
	}

	if err = in.decd.Decode(in.data); err != nil {
		return err
	}

	return nil
}

// move the file descriptor pointer to the beginning
func (in *Index) seekToStart() (err error) {
	o, err := in.file.Seek(0, 0)
	if err != nil {
		return err
	} else if o != 0 {
		return ErrOffsetMismatch
	}

	return nil
}

func (in *Index) Close() (err error) {
	err = in.file.Close()
	if err != nil {
		return err
	}

	return nil
}
