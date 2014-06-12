package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"sync"
	"time"

	"code.google.com/p/goprotobuf/proto"
	log "github.com/ngaut/logging"
	"mesos.apache.org/mesos"
)

type ShellExecutor struct {
	lock    sync.Mutex
	pwd     string
	finish  chan string
	driver  *mesos.ExecutorDriver
	process map[string]*exec.Cmd //taskid-pid
}

func (self *ShellExecutor) OnRegister(
	driver *mesos.ExecutorDriver,
	executor mesos.ExecutorInfo,
	framework mesos.FrameworkInfo,
	slave mesos.SlaveInfo) {
	fmt.Println("Executor Registered, executor id:", executor.GetExecutorId().GetValue())
	self.driver = driver
}

func (self *ShellExecutor) sendHeartbeat() {
	log.Debug("send heartbeat")
	for taskId, _ := range self.process {
		tid := taskId
		log.Debug("send heartbeat", tid)
		status := &mesos.TaskStatus{
			TaskId:  &mesos.TaskID{Value: &tid},
			State:   mesos.NewTaskState(mesos.TaskState_TASK_RUNNING),
			Message: proto.String("heartbeat..."),
			Data:    []byte(self.pwd),
		}
		self.driver.SendStatusUpdate(status)
	}
}

func (self *ShellExecutor) EventLoop() {
	tick := time.NewTicker(3 * time.Second)
	for {
		select {
		case taskId := <-self.finish:
			self.lock.Lock()
			delete(self.process, taskId) //thread safe
			self.lock.Unlock()
			tick.Stop()
			return
		case <-tick.C:
			self.lock.Lock()
			self.sendHeartbeat()
			self.lock.Unlock()
		}
	}
}

func (self *ShellExecutor) OnKillTask(driver *mesos.ExecutorDriver, tid mesos.TaskID) {
	taskId := tid.GetValue()
	log.Warningf("OnKillTask %s", taskId)
	self.lock.Lock()
	defer self.lock.Unlock()
	if cmd, ok := self.process[taskId]; ok {
		err := cmd.Process.Kill()
		if err != nil {
			log.Errorf("kill taskId %s failed, err:%v", taskId, err)
		}
	}

	log.Error("send kill state")

	driver.SendStatusUpdate(&mesos.TaskStatus{
		TaskId:  &tid,
		State:   mesos.NewTaskState(mesos.TaskState_TASK_KILLED),
		Message: proto.String("task killed by framework!"),
		Data:    []byte(self.pwd),
	})
}

func (self *ShellExecutor) OnLaunchTask(driver *mesos.ExecutorDriver, taskInfo mesos.TaskInfo) {
	fmt.Println("Launch task:", taskInfo.TaskId.GetValue())
	driver.SendStatusUpdate(&mesos.TaskStatus{
		TaskId:  taskInfo.TaskId,
		State:   mesos.NewTaskState(mesos.TaskState_TASK_RUNNING),
		Message: proto.String("Go task is running!"),
		Data:    []byte(self.pwd),
	})

	log.Debugf("%+v", os.Args)
	startch := make(chan struct{}, 1)
	if len(os.Args) == 2 {
		fname := taskInfo.TaskId.GetValue()
		ioutil.WriteFile(fname, []byte(os.Args[1]), 0644)
		cmd := exec.Command("/bin/sh", fname)
		go func() {
			defer func() {
				self.finish <- taskInfo.TaskId.GetValue()
				log.Debug("send finish state")

				driver.SendStatusUpdate(&mesos.TaskStatus{
					TaskId:  taskInfo.TaskId,
					State:   mesos.NewTaskState(mesos.TaskState_TASK_FINISHED),
					Message: proto.String("Go task is done!"),
					Data:    []byte(self.pwd),
				})
			}()

			self.lock.Lock()
			self.process[taskInfo.TaskId.GetValue()] = cmd
			self.lock.Unlock()
			startch <- struct{}{}
			out, err := cmd.Output()

			if err != nil {
				log.Error(err.Error())
			} else {
				fmt.Println(string(out))
				//	log.Debug(string(out))
			}
		}()
	} else {
		log.Debug("send finish state")
		driver.SendStatusUpdate(&mesos.TaskStatus{
			TaskId:  taskInfo.TaskId,
			State:   mesos.NewTaskState(mesos.TaskState_TASK_FINISHED),
			Message: proto.String("Go task is done!"),
			Data:    []byte(self.pwd),
		})
	}
	<-startch
}

func main() {
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	se := &ShellExecutor{pwd: pwd, finish: make(chan string),
		process: make(map[string]*exec.Cmd)}
	driver := mesos.ExecutorDriver{
		Executor: &mesos.Executor{
			Registered: se.OnRegister,
			KillTask:   se.OnKillTask,
			LaunchTask: se.OnLaunchTask,
		},
	}

	go se.EventLoop()

	driver.Init()
	defer driver.Destroy()

	driver.Run()
}
