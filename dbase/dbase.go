package dbase

import (
	"errors"
	"strings"
	"time"

	"github.com/meteorhacks/kdb"
	"github.com/meteorhacks/kdb/dbucket"
)

const (
	MaxHotBuckets = 2
)

var (
	ErrInvalidParams      = errors.New("invalid params")
	ErrInvalidTimestamp   = errors.New("value came from future")
	ErrInvalidPartition   = errors.New("partition number is invalid")
	ErrInvalidIndexValues = errors.New("invalid index values")
	ErrInvalidPayload     = errors.New("invalid payload size")
)

type Options struct {
	// database name. Currently only used with naming files
	// can be useful when supporting multiple Databases
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
}

type DBase struct {
	Options

	// A map of base bucket timestamps and pointers to buckets
	// only a preset number of buckets may be in memory at a time.
	// If maximum number of buckets exceeds `MaxHotBuckets`
	// the bucket with oldest base timestamp will be removed form memory
	Buckets map[int64]kdb.Bucket

	// empty slice with enough empty payloads to fill a bucket
	// used to fill result when bucket doesn't have required data
	emptyOut [][]byte
}

func New(opts Options) (db *DBase, err error) {
	if opts.BucketDuration%opts.Resolution != 0 {
		return nil, ErrInvalidParams
	}

	// pre compute empty result slices to use with Get/Find requests
	outSize := int(opts.BucketDuration / opts.Resolution)
	emptyOut := make([][]byte, outSize, outSize)
	emptyPld := make([]byte, opts.PayloadSize, opts.PayloadSize)
	for i := 0; i < outSize; i++ {
		emptyOut[i] = emptyPld
	}

	bkts := make(map[int64]kdb.Bucket, MaxHotBuckets)
	db = &DBase{opts, bkts, emptyOut}

	ts := time.Now().UnixNano()
	if _, err = db.getBucket(ts); err != nil {
		return nil, err
	}

	return db, nil
}

// Put adds new data points to the correct bucket.
// It also validates all incoming parameters before passing on to buckets
func (db *DBase) Put(ts int64, vals []string, pld []byte) (err error) {
	// floor tiemstamps by resolution
	ts -= ts % db.Resolution

	now := time.Now().UnixNano()
	if ts > now {
		return ErrInvalidTimestamp
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

	bkt, err := db.getBucket(ts)
	if err != nil {
		return err
	}

	err = bkt.Put(ts, vals, pld)
	if err != nil {
		return err
	}

	return nil
}

func (db *DBase) Get(start, end int64, vals []string) (res [][]byte, err error) {
	// floor tiemstamps by resolution
	start -= start % db.Resolution
	end -= end % db.Resolution

	now := time.Now().UnixNano()
	if start > now || end > now || end < start {
		return nil, ErrInvalidTimestamp
	}

	if len(vals) != int(db.IndexDepth) {
		return nil, ErrInvalidIndexValues
	}

	// base time of starting bucket
	bs := start - (start % db.BucketDuration)

	// base time of last bucket
	be := end - (end % db.BucketDuration)

	// number of payoads in final result
	rs := (end - start) / db.Resolution
	res = make([][]byte, 0, rs)

	var bktStart, bktEnd int64

	for t := bs; t <= be; t += db.BucketDuration {
		bkt, err := db.getBucket(t)
		if err != nil {
			return nil, err
		}

		if t == bs {
			// if it's the first bucket
			// skip payloads before `start` time
			bktStart = start
		} else {
			// defaults to base time of the bucket
			bktStart = t
		}

		// skip payloads after end time in end bucket
		if t == be {
			// if this is the last bucket
			// skip payloads after `end` time
			bktEnd = end
		} else {
			// defaults to end of the bucket
			bktEnd = t + db.BucketDuration
		}

		out, err := bkt.Get(bktStart, bktEnd, vals)
		if err != nil {
			return nil, err
		}

		if out == nil {
			count := (bktEnd - bktStart) / db.Resolution
			out = db.emptyOut[:count]
		}

		res = append(res, out...)
	}

	return res, nil
}

func (db *DBase) Find(start, end int64, vals []string) (res map[*kdb.IndexElement][][]byte, err error) {
	// floor tiemstamps by resolution
	start -= start % db.Resolution
	end -= end % db.Resolution

	now := time.Now().UnixNano()
	if start > now || end > now || end < start {
		return nil, ErrInvalidTimestamp
	}

	// base time of starting bucket
	bs := start - (start % db.BucketDuration)

	// base time of last bucket
	be := end - (end % db.BucketDuration)

	// number of payoads in final result
	rs := (end - start) / db.Resolution
	tmp := make(map[string][][]byte)
	tmpVals := make(map[string][]string)

	var bktStart, bktEnd int64

	for t := bs; t <= be; t += db.BucketDuration {
		bkt, err := db.getBucket(t)
		if err != nil {
			return nil, err
		}

		if t == bs {
			// if it's the first bucket
			// skip payloads before `start` time
			bktStart = start
		} else {
			// defaults to base time of the bucket
			bktStart = t
		}

		// skip payloads after end time in end bucket
		if t == be {
			// if this is the last bucket
			// skip payloads after `end` time
			bktEnd = end
		} else {
			// defaults to end of the bucket
			bktEnd = t + db.BucketDuration
		}

		out, err := bkt.Find(bktStart, bktEnd, vals)
		if err != nil {
			return nil, err
		}

		for el, plds := range out {
			key := strings.Join(el.Values, "-")

			set, ok := tmp[key]
			if !ok {
				set = make([][]byte, rs, rs)

				var i int64
				for i = 0; i < rs; i++ {
					set[i] = make([]byte, db.PayloadSize)
				}

				tmp[key] = set
				tmpVals[key] = el.Values
			}

			rStart := (bktStart - start) / db.Resolution
			rEnd := (bktEnd - start) / db.Resolution
			copy(set[rStart:rEnd], plds)
		}
	}

	// move data from tmp to res
	res = make(map[*kdb.IndexElement][][]byte)
	for key, val := range tmpVals {
		el := &kdb.IndexElement{Values: val}
		res[el] = tmp[key]
	}

	return res, nil
}

func (db *DBase) Close() (err error) {
	for _, bkt := range db.Buckets {
		err = bkt.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

// getBucket fetches a `Bucket` from the base timestamp => Bucket map
// If a bucket does not exist, it will be created and added to the map
func (db *DBase) getBucket(ts int64) (bkt kdb.Bucket, err error) {
	baseTS := ts - (ts % db.BucketDuration)
	if bkt, ok := db.Buckets[baseTS]; ok {
		return bkt, nil
	}

	bkt, err = dbucket.New(dbucket.Options{
		DatabaseName:   db.DatabaseName,
		DataPath:       db.DataPath,
		IndexDepth:     db.IndexDepth,
		PayloadSize:    db.PayloadSize,
		BucketDuration: db.BucketDuration,
		Resolution:     db.Resolution,
		BaseTime:       baseTS,
		SegmentSize:    db.SegmentSize,
	})

	if err != nil {
		return nil, err
	}

	db.Buckets[baseTS] = bkt
	// TODO: make sure hot bucket count is <= `MaxHotBuckets`
	//       for now, we're just loading all buckets to the ram

	return bkt, nil
}
