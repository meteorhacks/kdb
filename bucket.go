package kdb

type Bucket interface {
	Put(ts, pno int64, vals []string, pld []byte) (err error)
}
