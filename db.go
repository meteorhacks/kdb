package kdb

import (
	"encoding/json"
	"errors"
	"os"
)

const (
	FMode  = os.O_CREATE | os.O_RDWR
	FPerms = 0644
)

var (
	// TODO: improve error messages
	ErrOffsetMismatch = errors.New("incorrect offset")
	ErrBytesWritten   = errors.New("incorrect bytes written")
)

// ------------ //
//      DB      //
// ------------ //

// TODO: add all other options
type DBOpts struct {
	Path string // base path for index and data files
}

type DB struct {
	opts  DBOpts
	index *Index
	data  *Data
}

func NewDB(opts DBOpts) (db *DB, err error) {
	// TODO: init index and data
	in, err := NewIndex(IndexOpts{})
	if err != nil {
		return nil, err
	}

	dt, err := NewData(DataOpts{})
	if err != nil {
		return nil, err
	}

	db = &DB{opts, in, dt}

	return db, nil
}

// --------------- //
//      Index      //
// --------------- //

type IndexOpts struct {
	Path string // path to the index file
}

type Index struct {
	opts IndexOpts
	data map[string]int64 // TODO: replace with a tree
	file *os.File         // index file handler
	encd *json.Encoder    // use protobuff for better perf
	decd *json.Decoder    // use protobuff for better perf
}

func NewIndex(opts IndexOpts) (in *Index, err error) {
	dm := map[string]int64{}
	fd, err := os.OpenFile(opts.Path, FMode, FPerms)
	if err != nil {
		return nil, err
	}

	en := json.NewEncoder(fd)
	de := json.NewDecoder(fd)
	in = &Index{opts, dm, fd, en, de}

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

	if err = in.decd.Decode(&in.data); err != nil {
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

// -------------- //
//      Data      //
// -------------- //

type DataOpts struct {
	Path  string // path to the data file
	Size  int64  // byte size of a value item
	Count int64  // number of values per record
}

type Data struct {
	opts  DataOpts
	dsize int64    // byte size of the data file
	rsize int64    // byte size of a template
	rtemp []byte   // empty record template
	file  *os.File // data file handler
}

func NewData(opts DataOpts) (dt *Data, err error) {
	fd, err := os.OpenFile(opts.Path, FMode, FPerms)
	if err != nil {
		return nil, err
	}

	fi, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	ds := fi.Size()
	rs := opts.Size * opts.Count
	rt := make([]byte, rs, rs)
	dt = &Data{opts, ds, rs, rt, fd}

	return dt, nil
}

// creates a new empty record on the file and returns
// the byte offset from the beginning of the file
func (dt *Data) NewRecord() (o int64, err error) {
	n, err := dt.file.WriteAt(dt.rtemp, dt.dsize)
	if err != nil {
		return 0, err
	} else if n != int(dt.rsize) {
		return 0, ErrBytesWritten
	}

	o = dt.dsize
	dt.dsize += dt.rsize

	return o, nil
}

func (dt *Data) Read(o, l int64) (b []byte, err error) {
	c := l * dt.opts.Size
	b = make([]byte, c, c)
	n, err := dt.file.ReadAt(b, o)
	if err != nil {
		return b, err
	} else if n != int(c) {
		return b, ErrBytesWritten
	}

	return b, nil
}

func (dt *Data) Write(b []byte, o int64) (err error) {
	n, err := dt.file.WriteAt(b, o)
	if err != nil {
		return err
	} else if n != int(dt.opts.Size) {
		return ErrBytesWritten
	}

	return nil
}

func (dt *Data) Close() (err error) {
	err = dt.file.Close()
	if err != nil {
		return err
	}

	return nil
}
