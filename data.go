package kdb

import (
	"os"
	"runtime"
	"sync/atomic"
)

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
	o = dt.dsize
	atomic.AddInt64(&dt.dsize, dt.rsize)
	runtime.Gosched()

	n, err := dt.file.WriteAt(dt.rtemp, o)
	if err != nil {
		return 0, err
	} else if n != int(dt.rsize) {
		return 0, ErrBytesWritten
	}

	return o, nil
}

// Read reads `l` number of items starting from `o` offset
func (dt *Data) Read(o, l int64) (b [][]byte, err error) {
	c := l * dt.opts.Size * 2
	r := make([]byte, c, c)

	n, err := dt.file.ReadAt(r, o)
	if err != nil {
		return nil, err
	} else if n != int(c) {
		return nil, ErrBytesWritten
	}

	b = make([][]byte, l)

	var i int64
	for i = 0; i < l; i++ {
		b[i] = r[i*dt.opts.Size : (i+1)*dt.opts.Size]
	}

	return b, nil
}

func (dt *Data) Write(b []byte, o int64) (err error) {
	vsize := int(dt.opts.Size)
	if len(b) != vsize {
		return ErrWrongValueSize
	}

	n, err := dt.file.WriteAt(b, o)
	if err != nil {
		return err
	} else if n != vsize {
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
