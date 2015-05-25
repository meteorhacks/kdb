package kdb

import (
	"os"
	"testing"
)

func TestBucketWriteRead(t *testing.T) {
	defer os.Remove("/tmp/i1")
	in, err := NewIndex(IndexOpts{
		Path: "/tmp/i1",
		Keys: []string{"foo"},
	})

	if err != nil {
		t.Fatal(err)
	}

	defer in.Close()

	defer os.Remove("/tmp/d1")
	dt, err := NewData(DataOpts{
		Path:  "/tmp/d1",
		Size:  4,
		Count: 10,
	})

	if err != nil {
		t.Fatal(err)
	}

	defer dt.Close()

	bk, err := NewBucket(BucketOpts{
		BaseTS:     0,
		Duration:   100,
		Resolution: 10,
		Index:      in,
		Data:       dt,
	})

	if err != nil {
		t.Fatal(err)
	}

	i1 := map[string]string{"foo": "bar"}
	v11 := []byte("1111")
	v12 := []byte("1122")

	i2 := map[string]string{"foo": "baz"}
	v21 := []byte("2211")
	v22 := []byte("2222")

	bk.Write(i1, v11, 20)
	bk.Write(i1, v12, 30)

	bk.Write(i2, v21, 20)
	bk.Write(i2, v22, 30)

	r1, err := bk.Find(i1, 0, 50)
	if err != nil {
		t.Fatal(err)
	}

	if len(r1) != 1 {
		t.Fatalf("incorrect number of items in result")
	}

	r11 := r1[0]
	if len(r11.Query) != 1 || r11.Query["foo"] != "bar" {
		t.Fatalf("incorrect query")
	}

	if string(r11.Values[2]) != string(v11) ||
		string(r11.Values[3]) != string(v12) {
		t.Fatalf("incorrect values")
	}

	r2, err := bk.Find(i1, 0, 50)
	if err != nil {
		t.Fatal(err)
	}

	if len(r2) != 1 {
		t.Fatalf("incorrect number of items in result")
	}

	r21 := r2[0]
	if len(r21.Query) != 1 || r21.Query["foo"] != "bar" {
		t.Fatalf("incorrect query")
	}

	if string(r21.Values[2]) != string(v11) ||
		string(r21.Values[3]) != string(v12) {
		t.Fatalf("incorrect values")
	}
}

func BenchmarkBucketWrite(b *testing.B) {
	defer os.Remove("/tmp/i1")
	in, err := NewIndex(IndexOpts{
		Path: "/tmp/i1",
		Keys: []string{"foo"},
	})

	if err != nil {
		b.Fatal(err)
	}

	defer in.Close()

	defer os.Remove("/tmp/d1")
	dt, err := NewData(DataOpts{
		Path:  "/tmp/d1",
		Size:  4,
		Count: 10,
	})

	if err != nil {
		b.Fatal(err)
	}

	defer dt.Close()

	bk, err := NewBucket(BucketOpts{
		BaseTS:     0,
		Duration:   100,
		Resolution: 10,
		Index:      in,
		Data:       dt,
	})

	if err != nil {
		b.Fatal(err)
	}

	i1 := map[string]string{"foo": "bar"}
	v11 := []byte("1111")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = bk.Write(i1, v11, 20)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBucketRead(b *testing.B) {
	defer os.Remove("/tmp/i1")
	in, err := NewIndex(IndexOpts{
		Path: "/tmp/i1",
		Keys: []string{"foo"},
	})

	if err != nil {
		b.Fatal(err)
	}

	defer in.Close()

	defer os.Remove("/tmp/d1")
	dt, err := NewData(DataOpts{
		Path:  "/tmp/d1",
		Size:  4,
		Count: 10,
	})

	if err != nil {
		b.Fatal(err)
	}

	defer dt.Close()

	bk, err := NewBucket(BucketOpts{
		BaseTS:     0,
		Duration:   100,
		Resolution: 10,
		Index:      in,
		Data:       dt,
	})

	if err != nil {
		b.Fatal(err)
	}

	i1 := map[string]string{"foo": "bar"}
	v11 := []byte("1111")

	err = bk.Write(i1, v11, 20)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bk.Find(i1, 0, 50)
		if err != nil {
			b.Fatal(err)
		}
	}
}
