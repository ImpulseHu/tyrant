package main

import (
	"flag"

	log "github.com/ngaut/logging"
	"github.com/ngaut/tyrant/scheduler"
	"github.com/ngaut/tyrant/scheduler/mesosrel"
)

//"github.com/ngaut/tyrant/scheduler/resourceScheduler"
var (
	conf   = flag.String("conf", "./config.ini", "config file")
	master = flag.String("master", "localhost:5050", "Location of leading Mesos master")
)

func Init() {
	log.Debug(*conf)
	scheduler.InitConfig(*conf)
	scheduler.InitSharedDbMap()
}

func main() {
	flag.Parse()
	Init()
	resScheduler := mesosrel.NewResMan()
	go func() {
		log.Debug("master:", *master)
		resScheduler.Run(*master)
	}()

	go func() {
		scheduler.CheckAutoRunJobs(resScheduler)
	}()
	s := scheduler.NewServer(":9090", resScheduler)
	s.Serve()
}
