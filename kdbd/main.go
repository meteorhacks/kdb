package main

import (
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"

	"github.com/meteorhacks/kdb"
	"github.com/meteorhacks/kdb/kdbd/ddp-server"
)

type Config struct {
	// database name. Currently only used with naming files
	// can be useful when supporting multiple Databases
	DatabaseName string `json:"databaseName"`

	// place to store data files
	DataPath string `json:"dataPath"`

	// number of partitions to divide indexes
	Partitions int64 `json:"partitions"`

	// depth of the index tree
	IndexDepth int64 `json:"indexDepth"`

	// maximum payload size in bytes
	PayloadSize int64 `json:"payloadSize"`

	// bucket duration in nano seconds
	// this should be a multiple of `Resolution`
	BucketSize int64 `json:"bucketSize"`

	// bucket resolution in nano seconds
	Resolution int64 `json:"resolution"`

	// address to listen for ddp traffic (host:port)
	DDPAddress string `json:"ddpAddress"`
}

var (
	ErrMissingConfigFilePath = errors.New("config file path is missing")
)

func main() {
	config, err := readConfigFile()
	if err != nil {
		panic(err)
	}

	db, err := kdb.NewDefaultDatabase(kdb.DefaultDatabaseOpts{
		DatabaseName: config.DatabaseName,
		DataPath:     config.DataPath,
		Partitions:   config.Partitions,
		IndexDepth:   config.IndexDepth,
		PayloadSize:  config.PayloadSize,
		BucketSize:   config.BucketSize,
		Resolution:   config.Resolution,
	})

	if err != nil {
		panic(err)
	}

	s := ddp.NewServer(ddp.ServerOpts{
		Address:  config.DDPAddress,
		Database: db,
	})

	s.Listen()
}

func readConfigFile() (config *Config, err error) {
	file := flag.String("config", "", "config JSON file")
	flag.Parse()

	if *file == "" {
		return nil, ErrMissingConfigFilePath
	}

	data, err := ioutil.ReadFile(*file)
	if err != nil {
		return nil, err
	}

	config = &Config{}
	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}
