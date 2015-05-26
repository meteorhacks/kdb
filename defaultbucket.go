package kdb

import "strconv"

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
	BucketSize int64

	// bucket resolution in nano seconds
	Resolution int64

	// base timestamp
	BaseTime int64
}

type DefaultBucket struct {
	DefaultBucketOpts
	indexes []Index
	block   Block
}

func NewDefaultBucket(opts DefaultBucketOpts) (bkt *DefaultBucket, err error) {
	idxs := make([]Index, opts.Partitions)
	baseTimeStr := strconv.Itoa(int(opts.BaseTime))
	basePath := opts.DataPath + opts.DatabaseName + "_" + baseTimeStr

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
	pldCount := opts.BucketSize / opts.Resolution

	// e.g: /data/db_0000.block
	blkPath := basePath + ".block"

	blk, err := NewFixedBlock(FixedBlockOpts{
		FilePath:     blkPath,
		PayloadSize:  opts.PayloadSize,
		PayloadCount: pldCount,
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

	var ppos int64
	ppos = (ts % bkt.BucketSize) / bkt.Resolution

	err = bkt.block.Put(rpos, ppos, pld)
	if err != nil {
		return err
	}

	return nil
}
