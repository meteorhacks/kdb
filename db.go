package kdb

import (
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
