package kdb

type Database interface {
	Put(ts, pno int64, vals []string, pld []byte) (err error)
}
