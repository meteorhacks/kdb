package clock

import (
	"time"
)

var (
	C Clock
	R = &RealClock{}
	T = &TestClock{}
)

func init() {
	C = R
}

type Clock interface {
	Now() (now int64)
}

type RealClock struct {
}

func (c *RealClock) Now() (now int64) {
	return time.Now().UnixNano()
}

type TestClock struct {
	ts int64
}

func (c *TestClock) Now() (now int64) {
	return c.ts
}

func Now() (now int64) {
	return C.Now()
}

func UseTestClock(now int64) {
	C = T
	T.ts = now
}
