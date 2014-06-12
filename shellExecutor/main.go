package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"

	"code.google.com/p/goprotobuf/proto"
	log "github.com/ngaut/logging"
	"mesos.apache.org/mesos"
)

func main() {
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	driver := mesos.ExecutorDriver{
		Executor: &mesos.Executor{
			Registered: func(
				driver *mesos.ExecutorDriver,
				executor mesos.ExecutorInfo,
				framework mesos.FrameworkInfo,
				slave mesos.SlaveInfo) {
				fmt.Println("Executor registered!")
			},

			LaunchTask: func(driver *mesos.ExecutorDriver, taskInfo mesos.TaskInfo) {
				fmt.Println("Launch task!")
				driver.SendStatusUpdate(&mesos.TaskStatus{
					TaskId:  taskInfo.TaskId,
					State:   mesos.NewTaskState(mesos.TaskState_TASK_RUNNING),
					Message: proto.String("Go task is running!"),
					Data:    []byte(pwd),
				})

				log.Debugf("%+v", os.Args)
				if len(os.Args) == 2 {
					ioutil.WriteFile("./run.sh", []byte(os.Args[1]), 0644)
					cmd := exec.Command("/bin/sh", "./run.sh")
					out, err := cmd.Output()

					if err != nil {
						log.Error(err.Error())
					} else {
						println(string(out))
						//	log.Debug(string(out))
					}
				}

				driver.SendStatusUpdate(&mesos.TaskStatus{
					TaskId:  taskInfo.TaskId,
					State:   mesos.NewTaskState(mesos.TaskState_TASK_FINISHED),
					Message: proto.String("Go task is done!"),
					Data:    []byte(pwd),
				})
			},
		},
	}

	driver.Init()
	defer driver.Destroy()

	driver.Run()
}
