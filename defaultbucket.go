package kdb

import (
	"path"
	"strconv"
)

type DefaultBucketOpts struct {
	// database name. Currently only used with naming files
	// can be useful when supporting multiple DBs
	DatabaseName string

	// place to store data files
	DataPath string

	// number of partitions to divide indexes
	Partitions int64

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

	// base timestamp
	BaseTime int64
}

type DefaultBucket struct {
	DefaultBucketOpts
	indexes []Index
	block   Block
}

func NewDefaultBucket(opts DefaultBucketOpts) (bkt *DefaultBucket, err error) {
	// a map of partition number to indexes
	idxs := make([]Index, opts.Partitions)

	basePath := path.Join(
		opts.DataPath,
		opts.DatabaseName+"_"+strconv.Itoa(int(opts.BaseTime)),
	)

	var pno int64
	for pno = 0; pno < opts.Partitions; pno++ {
		pnoStr := strconv.Itoa(int(pno))

		// e.g: /data/db_0000_1.index
		idxPath := basePath + "_" + pnoStr + ".index"

		idxs[pno], err = NewMemIndex(MemIndexOpts{
			FilePath:    idxPath,
			IndexDepth:  opts.IndexDepth,
			PartitionNo: pno,
		})

		if err != nil {
			return nil, err
		}
	}

	// number of payloads in a record
	pldCount := opts.BucketDuration / opts.Resolution

	blk, err := NewFixedBlock(FixedBlockOpts{
		BlockPath:    basePath,
		PayloadSize:  opts.PayloadSize,
		PayloadCount: pldCount,
		SegmentSize:  opts.SegmentSize,
	})

	if err != nil {
		return nil, err
	}

	bkt = &DefaultBucket{opts, idxs, blk}
	return bkt, nil
}

// Put adds new data to correct index and block
func (bkt *DefaultBucket) Put(ts, pno int64, vals []string, pld []byte) (err error) {
	var rpos int64

	index := bkt.indexes[pno]
	el, err := index.Get(vals)
	if err != nil {
		return err
	}

	if el == nil {
		rpos, err = bkt.block.NewRecord()
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
func (bkt *DefaultBucket) Get(pno, start, end int64, vals []string) (res [][]byte, err error) {
	index := bkt.indexes[pno]

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
func (bkt *DefaultBucket) Find(pno, start, end int64, vals []string) (res map[*IndexElement][][]byte, err error) {
	res = make(map[*IndexElement][][]byte)

	index := bkt.indexes[pno]
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

func (bkt *DefaultBucket) tsToPPos(ts int64) (pos int64) {
	return (ts - bkt.BaseTime) / bkt.Resolution
}
