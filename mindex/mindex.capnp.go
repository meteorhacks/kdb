package mindex

// AUTO GENERATED - DO NOT EDIT

import (
	C "github.com/glycerine/go-capnproto"
)

type MIndexEl C.Struct

func NewMIndexEl(s *C.Segment) MIndexEl      { return MIndexEl(s.NewStruct(8, 1)) }
func NewRootMIndexEl(s *C.Segment) MIndexEl  { return MIndexEl(s.NewRootStruct(8, 1)) }
func AutoNewMIndexEl(s *C.Segment) MIndexEl  { return MIndexEl(s.NewStructAR(8, 1)) }
func ReadRootMIndexEl(s *C.Segment) MIndexEl { return MIndexEl(s.Root(0).ToStruct()) }
func (s MIndexEl) Position() int64           { return int64(C.Struct(s).Get64(0)) }
func (s MIndexEl) SetPosition(v int64)       { C.Struct(s).Set64(0, uint64(v)) }
func (s MIndexEl) Values() C.TextList        { return C.TextList(C.Struct(s).GetObject(0)) }
func (s MIndexEl) SetValues(v C.TextList)    { C.Struct(s).SetObject(0, C.Object(v)) }

// capn.JSON_enabled == false so we stub MarshallJSON().
func (s MIndexEl) MarshalJSON() (bs []byte, err error) { return }

type MIndexEl_List C.PointerList

func NewMIndexElList(s *C.Segment, sz int) MIndexEl_List {
	return MIndexEl_List(s.NewCompositeList(8, 1, sz))
}
func (s MIndexEl_List) Len() int          { return C.PointerList(s).Len() }
func (s MIndexEl_List) At(i int) MIndexEl { return MIndexEl(C.PointerList(s).At(i).ToStruct()) }
func (s MIndexEl_List) ToArray() []MIndexEl {
	n := s.Len()
	a := make([]MIndexEl, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s MIndexEl_List) Set(i int, item MIndexEl) { C.PointerList(s).Set(i, C.Object(item)) }
