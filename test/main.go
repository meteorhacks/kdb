package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"log"
	"os"

	"github.com/meteorhacks/kdb"
	"gopkg.in/meteorhacks/goddp.v1/server"

	"net/http"
	_ "net/http/pprof"
)

type Point struct {
	Type    string  `json:"type"`
	Time    int64   `json:"time"`
	Value   float64 `json:"value"`
	Samples float64 `json:"samples"`
}

var (
	bk *kdb.Bucket
)

func main() {
	defer os.Remove("/data/i1")
	in, err := kdb.NewIndex(kdb.IndexOpts{
		Path: "/data/i1",
		Keys: []string{"type"},
	})

	if err != nil {
		panic(err)
	}

	defer in.Close()

	defer os.Remove("/data/d1")
	dt, err := kdb.NewData(kdb.DataOpts{
		Path:  "/data/d1",
		Size:  16,
		Count: 1440,
	})

	if err != nil {
		panic(err)
	}

	defer dt.Close()

	bk, err = kdb.NewBucket(kdb.BucketOpts{
		BaseTS:     0,
		Duration:   1440,
		Resolution: 1,
		Index:      in,
		Data:       dt,
	})

	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	s := server.New()
	s.Method("write", handler)
	s.Listen(":80")
}

func handler(ctx server.MethodContext) {
	defer ctx.SendUpdated()

	data, ok := ctx.Params[0].(string)
	if !ok {
		ctx.SendError("missing payload")
		return
	}

	var points []Point
	json.Unmarshal([]byte(data), &points)

	buf := new(bytes.Buffer)
	item := map[string]string{"type": ""}

	for _, p := range points {
		if err := binary.Write(buf, binary.LittleEndian, p.Value); err != nil {
			ctx.SendError(err.Error())
			return
		}

		if err := binary.Write(buf, binary.LittleEndian, p.Samples); err != nil {
			ctx.SendError(err.Error())
			return
		}

		item["type"] = p.Type

		if err := bk.Write(item, buf.Bytes(), p.Time); err != nil {
			ctx.SendError(err.Error())
			return
		}

		buf.Reset()
	}

	ctx.SendResult(nil)
}
