package kdb

import (
	"encoding/json"
	"os"
)

type IndexOpts struct {
	Keys []string // index keys
	Path string   // path to the index file
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
