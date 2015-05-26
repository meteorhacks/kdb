package ddp

import (
	"encoding/base64"
	"encoding/json"
	"log"

	"github.com/meteorhacks/goddp/server"
	"github.com/meteorhacks/kdb"
)

type WriteRequestPoint struct {
	Partition int64    `json:"partition"`
	Timestamp int64    `json:"timestamp"`
	IndexVals []string `json:"indexValues"`
	Payload   string   `json:"payload"`
}

type WriteRequest struct {
	Points []WriteRequestPoint `json:"points"`
}

type ServerOpts struct {
	Address  string
	Database kdb.Database
}

type Server struct {
	ServerOpts
}

func NewServer(opts ServerOpts) (s *Server) {
	return &Server{opts}
}

func (s *Server) Listen() {
	log.Print("DDPServer: listening on ", s.Address)
	ddp := server.New()
	ddp.Method("put", s.handlePut)
	ddp.Listen(s.Address)
}

func (s *Server) handlePut(ctx server.MethodContext) {
	defer ctx.SendUpdated()

	reqData, ok := ctx.Params[0].(string)
	if !ok {
		ctx.SendError("missing payload")
		return
	}

	var req *WriteRequest
	json.Unmarshal([]byte(reqData), &req)

	for _, p := range req.Points {
		payload, err := base64.StdEncoding.DecodeString(p.Payload)
		if err != nil {
			ctx.SendError(err.Error())
			return
		}

		err = s.Database.Put(p.Timestamp, p.Partition, p.IndexVals, payload)
		if err != nil {
			ctx.SendError(err.Error())
			return
		}
	}

	ctx.SendResult(nil)
}
