package dbucket

import (
	"errors"
	"os"
	"path"
	"strconv"

	"github.com/meteorhacks/kdb"
	"github.com/meteorhacks/kdb/dblock"
	"github.com/meteorhacks/kdb/mindex"
	"github.com/meteorhacks/kdb/rblock"
)

const (
	FilePermissions = 0744
)

var (
	ErrWriteOnReadOnly = errors.New("write operation on a read only bucket")
)

type Options struct {
	// database name. Currently only used with naming files
	// can be useful when supporting multiple DBs
	DatabaseName string

	// place to store data files
	DataPath string

	// depth of the index tree
	IndexDepth int64

	// maximum payload size in bytes
	PayloadSize int64

	// bucket duration in nano seconds
	// this should be a multiple of `Resolution`
	BucketDuration int64

	// bucket resolution in nano seconds
	Resolution int64

	// number of records per segment
	SegmentSize int64

	// read only bucket (less RAM usage)
	ReadOnly bool

	// base timestamp
	BaseTime int64
}

type DBucket struct {
	Options
	index kdb.Index
	block kdb.Block
}

func New(opts Options) (bkt *DBucket, err error) {
	basePath := path.Join(
		opts.DataPath,
		opts.DatabaseName+"_"+strconv.Itoa(int(opts.BaseTime)),
	)

	err = os.MkdirAll(basePath, FilePermissions)
	if err != nil {
		return nil, err
	}

	idxPath := path.Join(basePath, "index")
	index, err := mindex.NewMIndex(mindex.MIndexOpts{
		FilePath:   idxPath,
		IndexDepth: opts.IndexDepth,
	})

	if err != nil {
		return nil, err
	}

	// number of payloads in a record
	pldCount := opts.BucketDuration / opts.Resolution
	var block kdb.Block

	if opts.ReadOnly {
		block, err = rblock.New(rblock.Options{
			BlockPath:    basePath,
			PayloadSize:  opts.PayloadSize,
			PayloadCount: pldCount,
			SegmentSize:  opts.SegmentSize,
		})
	} else {
		block, err = dblock.New(dblock.Options{
			BlockPath:    basePath,
			PayloadSize:  opts.PayloadSize,
			PayloadCount: pldCount,
			SegmentSize:  opts.SegmentSize,
		})
	}

	if err != nil {
		return nil, err
	}

	bkt = &DBucket{opts, index, block}
	return bkt, nil
}

// Put adds new data to correct index and block
func (bkt *DBucket) Put(ts int64, vals []string, pld []byte) (err error) {
	if bkt.ReadOnly {
		return ErrWriteOnReadOnly
	}

	var rpos int64

	index := bkt.index
	el, err := index.Get(vals)
	if err != nil {
		return err
	}

	if el == nil {
		rpos, err = bkt.block.New()
		if err != nil {
			return err
		}

		el, err = index.Add(vals, rpos)
		if err != nil {
			return err
		}
	}

	ppos := bkt.tsToPPos(ts)

	err = bkt.block.Put(rpos, ppos, pld)
	if err != nil {
		return err
	}

	return nil
}

// Get method gets the payload for matching value set
func (bkt *DBucket) Get(start, end int64, vals []string) (res [][]byte, err error) {
	index := bkt.index

	el, err := index.Get(vals)
	if err != nil {
		return nil, err
	}

	if el == nil {
		return nil, nil
	}

	spos := bkt.tsToPPos(start)
	epos := bkt.tsToPPos(end)

	// if data is not available
	// send an empty payload
	if el == nil {
		size := int(epos - spos)
		pld := make([]byte, bkt.PayloadSize, bkt.PayloadSize)
		res = make([][]byte, size, size)

		for i := 0; i < size; i++ {
			res[i] = pld
		}

		return res, nil
	}

	res, err = bkt.block.Get(el.Position, spos, epos)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// Find method finds all payloads matching the given query
func (bkt *DBucket) Find(start, end int64, vals []string) (res map[*kdb.IndexElement][][]byte, err error) {
	res = make(map[*kdb.IndexElement][][]byte)

	index := bkt.index
	els, err := index.Find(vals)
	if err != nil {
		return nil, err
	}

	for _, el := range els {
		spos := bkt.tsToPPos(start)
		epos := bkt.tsToPPos(end)
		res[el], err = bkt.block.Get(el.Position, spos, epos)
		if err != nil {
			return nil, err
		}
	}

	return res, nil
}

func (bkt *DBucket) Close() (err error) {
	err = bkt.index.Close()
	if err != nil {
		return err
	}

	err = bkt.block.Close()
	if err != nil {
		return err
	}

	return nil
}

func (bkt *DBucket) tsToPPos(ts int64) (pos int64) {
	return (ts - bkt.BaseTime) / bkt.Resolution
}
