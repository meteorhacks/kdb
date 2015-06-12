using Go = import "../../../glycerine/go-capnproto/go.capnp";

@0xa523262618bbae05;
$Go.package("mindex");
$Go.import("github.com/glycerine/go-capnproto/capnpc-go");

using Id = UInt64;

struct MIndexEl {
  position @0 :Int64;
  values @1 :List(Text);
}
