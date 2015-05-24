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
	ErrOffsetMismatch   = errors.New("incorrect offset")
	ErrBytesWritten     = errors.New("incorrect bytes written")
	ErrWrongValueSize   = errors.New("incorrect value size")
	ErrTSOutOfRange     = errors.New("timestamp out of range")
	ErrTSIsInvalid      = errors.New("timestamp range is not valid")
	ErrMissingKeyFields = errors.New("missing key fields")
)

type ResultItem struct {
	Query  map[string]string
	Values [][]byte
}

type BucketOpts struct {
	BaseTS     int64  // base time in nano seconds
	Duration   int64  // duration in ns to hold in a record
	Resolution int64  // resolution in ns of values
	Index      *Index // index store
	Data       *Data  // data store
}

type Bucket struct {
	opts  BucketOpts
	Index *Index
	Data  *Data
	maxTS int64
}

// NewBucket creates a Bucket with given Index and Data
func NewBucket(opts BucketOpts) (bk *Bucket, err error) {
	maxTS := opts.BaseTS + opts.Duration - opts.Resolution
	bk = &Bucket{opts, opts.Index, opts.Data, maxTS}
	return bk, nil
}

func (bk *Bucket) Write(item map[string]string, b []byte, ts int64) (err error) {
	rOffset, err := bk.tsToOffset(ts)
	if err != nil {
		return err
	}

	keys := bk.Index.opts.Keys
	req := len(keys)

	// make sure item has all required keys
	for _, k := range keys {
		if _, ok := item[k]; ok {
			req--
		}
	}

	if req > 0 {
		return ErrMissingKeyFields
	}

	// find record base els using the index
	el, err := bk.Index.Get(item)
	if err != nil {
		return err
	}

	if el == nil {
		rPos, err := bk.Data.NewRecord()
		if err != nil {
			return err
		}

		el, err = bk.Index.AddItem(item, rPos)
		if err != nil {
			return err
		}
	}

	o := el.Position + rOffset*bk.Data.opts.Size
	err = bk.Data.Write(b, o)
	if err != nil {
		return err
	}

	return nil
}

func (bk *Bucket) Find(query map[string]string, from, to int64) (out []*ResultItem, err error) {
	// convert `from` timestamp to byte offset from record base
	f, err := bk.tsToOffset(from)
	if err != nil {
		return nil, err
	}

	// convert `to` timestamp to byte offset from record base
	t, err := bk.tsToOffset(to)
	if err != nil {
		return nil, err
	}

	// validate the range
	if f == t {
		t = f + bk.opts.Resolution
	} else if f > t {
		return nil, ErrTSIsInvalid
	}

	// calculate the number of bytes to read from `from` offset
	// and number of values in result set
	l := t - f

	// find record base els using the index
	els, err := bk.Index.Find(query)
	if err != nil {
		return nil, err
	}

	out = make([]*ResultItem, len(els))
	for i, el := range els {
		res := &ResultItem{}
		if res.Query, err = bk.Index.MakeQuery(el); err != nil {
			return nil, err
		}

		o := el.Position + f*bk.Data.opts.Size
		if res.Values, err = bk.Data.Read(o, l); err != nil {
			return nil, err
		}

		out[i] = res
	}

	return out, nil
}

func (bk *Bucket) tsToOffset(ts int64) (o int64, err error) {
	if ts < bk.opts.BaseTS || ts > bk.maxTS {
		return 0, ErrTSOutOfRange
	}

	ts -= ts % bk.opts.Resolution
	o = (ts - bk.opts.BaseTS) / bk.opts.Resolution

	return o, nil
}
