package main

import (
	"flag"
	"strings"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/c4pt0r/cfg"
	log "github.com/ngaut/logging"
	"github.com/ngaut/tyrant/scheduler"
	"github.com/ngaut/tyrant/scheduler/mesosrel"
	"github.com/ngaut/tyrant/zkhelper"
	"github.com/samuel/go-zookeeper/zk"
)

var (
	conf   = flag.String("conf", "./config.ini", "config file")
	master = flag.String("master", "localhost:5050", "Location of leading Mesos master")
)

func Init() {
	log.Debug(*conf)
	scheduler.InitConfig(*conf)
	scheduler.InitSharedDbMap()
}

const (
	tyrant_zk_path = "/zk/tyrant/"
)

func tryRunAsLeader() {
	config := cfg.NewCfg(*conf)
	err := config.Load()
	if err != nil {
		log.Fatal(err)
	}
	zkaddr, err := config.ReadString("zk", "localhost:2181/tyrant")
	log.Debug("zk:", zkaddr)

	zkConn, _, err := zk.Connect(strings.Split(zkaddr, ","), 3*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	leader := zkhelper.CreateElection(*zkConn, tyrant_zk_path)
	task := &LeaderTask{}
	leader.RunTask(task)
}

type LeaderTask struct {
}

func (self *LeaderTask) Run() error {
	resScheduler := mesosrel.NewResMan()
	go func() {
		log.Debug("master:", *master)
		resScheduler.Run(*master)
	}()

	go func() {
		scheduler.CheckAutoRunJobs(resScheduler)
	}()

	go func() {
		http.ListenAndServe(":9091", nil)
	}()

	s := scheduler.NewServer(":9090", resScheduler)
	s.Serve()

	log.Fatal("End of Run()")

	return nil
}

func (self *LeaderTask) Stop() {
	log.Fatal("stop server")
}

func (self *LeaderTask) Interrupted() bool {
	log.Fatal("Interrupted")

	return false
}

func main() {
	flag.Parse()
	Init()
	tryRunAsLeader()
}
