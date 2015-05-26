package kdb

import (
	"errors"
	"time"
)

const (
	DefaultDatabaseMaxHotBuckets = 2
)

var (
	ErrValueIsFromFuture  = errors.New("value came from future")
	ErrInvalidPartition   = errors.New("partition number is invalid")
	ErrInvalidIndexValues = errors.New("invalid index values")
	ErrInvalidPayload     = errors.New("invalid payload size")
)

type DefaultDatabaseOpts struct {
	// database name. Currently only used with naming files
	// can be useful when supporting multiple Databases
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
}

type DefaultDatabase struct {
	DefaultDatabaseOpts

	// A map of base bucket timestamps and pointers to buckets
	// only a preset number of buckets may be in memory at a time.
	// If maximum number of buckets exceeds `DefaultDatabaseMaxHotBuckets`
	// the bucket with oldest base timestamp will be removed form memory
	Buckets map[int64]Bucket
}

func NewDefaultDatabase(opts DefaultDatabaseOpts) (db *DefaultDatabase, err error) {
	bkts := make(map[int64]Bucket, DefaultDatabaseMaxHotBuckets)
	db = &DefaultDatabase{opts, bkts}

	// init latest bucket
	ts := time.Now().UnixNano()
	_, err = db.GetBucket(ts)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// Put adds new data points to the correct bucket.
// It also validates all incoming parameters before passing on to buckets
func (db *DefaultDatabase) Put(ts, pno int64, vals []string, pld []byte) (err error) {
	now := time.Now().UnixNano()
	if ts > now {
		return ErrValueIsFromFuture
	}

	if pno < 0 || pno > db.Partitions {
		return ErrInvalidPartition
	}

	if len(vals) != int(db.IndexDepth) {
		return ErrInvalidIndexValues
	}

	for _, v := range vals {
		if v == "" {
			return ErrInvalidIndexValues
		}
	}

	if len(pld) != int(db.PayloadSize) {
		return ErrInvalidPayload
	}

	bkt, err := db.GetBucket(ts)
	if err != nil {
		return err
	}

	err = bkt.Put(ts, pno, vals, pld)
	if err != nil {
		return err
	}

	return nil
}

// GetBucket fetches a `Bucket` from the base timestamp => Bucket map
// If a bucket does not exist, it will be created and added to the map
func (db *DefaultDatabase) GetBucket(ts int64) (bkt Bucket, err error) {
	baseTS := ts - (ts % db.BucketSize)
	if bkt, ok := db.Buckets[baseTS]; ok {
		return bkt, nil
	}

	bkt, err = NewDefaultBucket(DefaultBucketOpts{
		DatabaseName: db.DefaultDatabaseOpts.DatabaseName,
		DataPath:     db.DefaultDatabaseOpts.DataPath,
		Partitions:   db.DefaultDatabaseOpts.Partitions,
		IndexDepth:   db.DefaultDatabaseOpts.IndexDepth,
		PayloadSize:  db.DefaultDatabaseOpts.PayloadSize,
		BucketSize:   db.DefaultDatabaseOpts.BucketSize,
		Resolution:   db.DefaultDatabaseOpts.Resolution,
		BaseTime:     baseTS,
	})

	if err != nil {
		return nil, err
	}

	db.Buckets[baseTS] = bkt
	// TODO: make sure hot bucket count is <= `DefaultDatabaseMaxHotBuckets`
	//       for now, we're just loading all buckets to the ram

	return bkt, nil
}
