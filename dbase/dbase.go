package dbase

import (
	"errors"
	"io/ioutil"
	"os/exec"
	"path"
	"strconv"
	"strings"

	"github.com/meteorhacks/kdb"
	"github.com/meteorhacks/kdb/clock"
	"github.com/meteorhacks/kdb/dbucket"
	"github.com/meteorhacks/kdb/queue"
)

const (
	MaxHotBuckets  = 2
	MaxColdBuckets = 4
)

var (
	ErrInvalidParams      = errors.New("invalid params")
	ErrInvalidTimestamp   = errors.New("value came from future")
	ErrInvalidIndexValues = errors.New("invalid index values")
	ErrInvalidPayload     = errors.New("invalid payload size")
	ErrRemoveHotBucket    = errors.New("can't remove hot bucket")
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
	HBuckets queue.Queue
	CBuckets queue.Queue

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

	hbkts := queue.NewQueue(MaxHotBuckets)
	cbkts := queue.NewQueue(MaxColdBuckets)
	db = &DBase{opts, hbkts, cbkts, emptyOut}

	now := clock.Now()
	now -= now % db.BucketDuration

	minHot := now - opts.BucketDuration*(MaxHotBuckets-1)
	minCold := minHot - opts.BucketDuration*MaxColdBuckets

	// int64 loop
	var i int64

	// load past few blocks as hot buckets
	// only these will perform writes
	for i = 0; i < MaxHotBuckets; i++ {
		ts := minHot + i*opts.BucketDuration
		if _, err = db.getBucket(ts); err != nil {
			return nil, err
		}
	}

	// assuming buckets immediately before earliest hot bucket
	// will most probably will be used, load them as cold buckets
	// this will load only if buckets already exist on the server
	for i = 0; i < MaxColdBuckets; i++ {
		ts := minCold + i*opts.BucketDuration
		if _, err = db.getBucket(ts); err != nil &&
			err != dbucket.ErrBucketNotInDisk {
			return nil, err
		}
	}

	// start a goroutine to close
	// all overflowing buckets
	go db.checkBucketCounts()

	return db, nil
}

// Put adds new data points to the correct bucket.
// It also validates all incoming parameters before passing on to buckets
func (db *DBase) Put(ts int64, vals []string, pld []byte) (err error) {
	// floor tiemstamps by resolution
	ts -= ts % db.Resolution

	now := clock.Now()
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

	now := clock.Now()
	last := end - db.Resolution
	if start > now || last > now || end < start {
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

		count := (bktEnd - bktStart) / db.Resolution
		out := db.emptyOut[:count]

		bkt, err := db.getBucket(t)
		if err != nil && err != dbucket.ErrBucketNotInDisk {
			return nil, err
		}

		if err == nil {
			out, err = bkt.Get(bktStart, bktEnd, vals)
			if err != nil {
				return nil, err
			}

			if out == nil {
				out = db.emptyOut[:count]
			}
		}

		res = append(res, out...)
	}

	return res, nil
}

func (db *DBase) Find(start, end int64, vals []string) (res map[*kdb.IndexElement][][]byte, err error) {
	// floor tiemstamps by resolution
	start -= start % db.Resolution
	end -= end % db.Resolution

	now := clock.Now()
	if start > now || end > now || end < start {
		return nil, ErrInvalidTimestamp
	}

	// base time of starting bucket
	bs := start - (start % db.BucketDuration)

	// base time of last bucket
	be := end - (end % db.BucketDuration)

	// number of payoads in final result
	rs := (end - start) / db.Resolution
	tmpData := make(map[string][][]byte)
	tmpVals := make(map[string][]string)

	var bktStart, bktEnd int64

	for t := bs; t <= be; t += db.BucketDuration {
		bkt, err := db.getBucket(t)
		notExist := err == dbucket.ErrBucketNotInDisk
		if err != nil {
			if notExist {
				continue
			}

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

			set, ok := tmpData[key]
			if !ok {
				set = make([][]byte, rs, rs)

				var i int64
				for i = 0; i < rs; i++ {
					set[i] = make([]byte, db.PayloadSize)
				}

				tmpData[key] = set
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
		res[el] = tmpData[key]
	}

	return res, nil
}

func (db *DBase) RemoveBefore(ts int64) (err error) {
	now := clock.Now()
	now -= now % db.BucketDuration
	min := now - db.BucketDuration*(MaxHotBuckets-1)
	pfx := db.DatabaseName + "_"

	if ts > min {
		return ErrRemoveHotBucket
	}

	files, _ := ioutil.ReadDir(db.DataPath)
	for _, f := range files {
		name := f.Name()

		if !strings.HasPrefix(name, pfx) {
			continue
		}

		tsStr := strings.TrimPrefix(name, pfx)
		tsInt, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			return err
		}

		if tsInt >= ts {
			continue
		}

		_, err = db.CBuckets.Del(tsInt)
		if err != nil && err != queue.ErrKeyMissing {
			return err
		}

		bpath := path.Join(db.DataPath, name)
		cmd := exec.Command("rm", "-rf", bpath)
		if err := cmd.Run(); err != nil {
			return err
		}
	}

	return nil
}

func (db *DBase) Close() (err error) {
	for _, val := range db.HBuckets.Flush() {
		bkt := val.(kdb.Bucket)
		err = bkt.Close()
		if err != nil {
			return err
		}
	}

	for _, val := range db.CBuckets.Flush() {
		bkt := val.(kdb.Bucket)
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

	// if a "hot" bucket is available, return the bucket
	if val, err := db.HBuckets.Get(baseTS); err == nil {
		bkt := val.(kdb.Bucket)
		return bkt, nil
	}

	// if a "cold" bucket is available, return the bucket
	if val, err := db.CBuckets.Get(baseTS); err == nil {
		bkt := val.(kdb.Bucket)
		return bkt, nil
	}

	nowTS := clock.Now()
	nowTS -= (nowTS % db.BucketDuration)
	minTS := nowTS - db.BucketDuration*MaxHotBuckets
	isHot := baseTS > minTS

	opts := dbucket.Options{
		DatabaseName:   db.DatabaseName,
		DataPath:       db.DataPath,
		IndexDepth:     db.IndexDepth,
		PayloadSize:    db.PayloadSize,
		BucketDuration: db.BucketDuration,
		Resolution:     db.Resolution,
		BaseTime:       baseTS,
		SegmentSize:    db.SegmentSize,
	}

	bkts := db.HBuckets

	if !isHot {
		opts.ReadOnly = true
		bkts = db.CBuckets
	}

	bkt, err = dbucket.New(opts)
	if err != nil {
		return nil, err
	}

	bkts.Add(baseTS, bkt)

	return bkt, nil
}

func (db *DBase) checkBucketCounts() {
	for {
		var val interface{}

		select {
		case val = <-db.HBuckets.Out():
		case val = <-db.CBuckets.Out():
		}

		bkt := val.(kdb.Bucket)
		if err := bkt.Close(); err != nil {
			// handle this error
			panic(err)
		}
	}
}
