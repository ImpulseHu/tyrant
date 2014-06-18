package main

import (
	"flag"
	"os"
	"strings"
	"time"

	"github.com/c4pt0r/cfg"
	log "github.com/ngaut/logging"
	"github.com/ngaut/tyrant/scheduler"
	"github.com/ngaut/tyrant/scheduler/mesosrel"
	myzk "github.com/ngaut/tyrant/zk"
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
	tyrant_zk_path    = "/hosts/"
	tyrant_tmp_master = tyrant_zk_path + "master"
)

func promote() {
	config := cfg.NewCfg(*conf)
	err := config.Load()
	if err != nil {
		log.Fatal(err)
	}
	zkaddr, err := config.ReadString("zk", "localhost:2181/tyrant")
	log.Debug(zkaddr)

	zkConn, err := myzk.Connect(strings.Split(zkaddr, ","), 3*time.Second)
	if err != nil {
		log.Fatal(err)
	}

	if exist, _, err := zkConn.Exists(tyrant_zk_path); err == nil && !exist {
		err = myzk.Create(zkConn, tyrant_zk_path)
		if err != nil && err != zk.ErrNodeExists {
			log.Fatal(err)
		}
	}

	hostName, _ := os.Hostname()
	if exist, _, err := zkConn.Exists(tyrant_tmp_master); err == nil && !exist {
		err = myzk.RegisterTemp(zkConn, tyrant_tmp_master, hostName)
		if err != nil && err != zk.ErrNodeExists {
			log.Fatal(err)
		}

		return
	}

	if err != nil {
		log.Fatal(err)
	}

	//watch it
	_, _, evtCh, err := zkConn.GetW(tyrant_tmp_master)
	if err != nil {
		log.Fatal(err)
	}

	for {
		evt := <-evtCh
		log.Fatalf("%+v", evt)
		switch evt.Type {
		case zk.EventNodeDeleted:
			err = myzk.RegisterTemp(zkConn, tyrant_tmp_master, hostName)
			if err != nil && err != myzk.ErrNodeNotExist {
				log.Fatal(err)
			}
			if err == nil {
				return
			}
		default:
			log.Fatalf("%+v", evt)
		}
	}
}

func main() {
	flag.Parse()
	Init()

	promote()

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
