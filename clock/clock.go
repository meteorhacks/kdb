package clock

import (
	"time"
)

var (
	C Clock = R
	R       = &RealClock{}
	T       = &TestClock{}
)

type Clock interface {
	Now() (ts int64)
}

type RealClock struct {
}

func (c *RealClock) Now() (ts int64) {
	return time.Now().UnixNano()
}

type TestClock struct {
	ts int64
}

func (c *TestClock) Now() (ts int64) {
	return c.ts
}

func Now() (ts int64) {
	return C.Now()
}

func UseRealClock() {
	C = R
}

func UseTestClock() {
	C = T
}

func Goto(ts int64) {
	T.ts = ts
}
