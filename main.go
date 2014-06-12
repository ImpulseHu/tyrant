package main

import (
	"github.com/ngaut/tyrant/scheduler"
	"github.com/ngaut/tyrant/scheduler/mesosrel"
)

//"github.com/ngaut/tyrant/scheduler/resourceScheduler"

func init() {
	scheduler.InitConfig("config.ini")
	scheduler.InitSharedDbMap()
}

func main() {
	resScheduler := mesosrel.NewResMan()
	go func() {
		resScheduler.Run()
	}()
	go func() {
		scheduler.CheckAutoRunJobs(resScheduler)
	}()
	s := scheduler.NewServer(":9090", resScheduler)
	s.Serve()
}
