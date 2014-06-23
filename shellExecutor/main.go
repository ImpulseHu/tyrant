package main

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"code.google.com/p/goprotobuf/proto"
	"github.com/ActiveState/tail"
	log "github.com/ngaut/logging"
	"mesos.apache.org/mesos"
)

type contex struct {
	cmd        *exec.Cmd
	statusFile *tail.Tail
}

type ShellExecutor struct {
	lock    sync.Mutex
	pwd     string
	finish  chan string
	driver  *mesos.ExecutorDriver
	process map[string]*contex //taskid-pid
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
	for taskId, _ := range self.process {
		tid := taskId
		log.Debug("send heartbeat, taskId", tid)
		self.sendStatusUpdate(tid, mesos.TaskState_TASK_RUNNING, "")
	}
}

func (self *ShellExecutor) EventLoop() {
	tick := time.NewTicker(10 * time.Second)
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
	if contex, ok := self.process[taskId]; ok {
		log.Debug("pid", contex.cmd.Process.Pid)
		ret, err := exec.Command("pkill", "-TERM", "-P", strconv.Itoa(contex.cmd.Process.Pid)).Output()
		if err != nil {
			log.Errorf("kill taskId %s failed, err:%v", taskId, err)
		}
		log.Debugf("kill taskId %s result %v", taskId, ret)
		contex.statusFile.Stop()
	}

	//log.Error("send kill state")
	//self.sendStatusUpdate(tid.GetValue(), mesos.TaskState_TASK_KILLED, "")
}

func (self *ShellExecutor) sendStatusUpdate(taskId string, state mesos.TaskState, message string) {
	self.driver.SendStatusUpdate(&mesos.TaskStatus{
		TaskId:  &mesos.TaskID{Value: &taskId},
		State:   mesos.NewTaskState(state),
		Message: proto.String(message),
		Data:    []byte(self.pwd), //todo: using FrameworkMessage
	})
}

func (self *ShellExecutor) tailf(fileName string, taskId string) *tail.Tail {
	t, err := tail.TailFile(fileName, tail.Config{Follow: true, ReOpen: true})
	if err != nil {
		log.Fatal(err)
		return nil
	}

	go func() {
		for line := range t.Lines {
			fmt.Println(line.Text)
			self.sendStatusUpdate(taskId, mesos.TaskState_TASK_RUNNING, line.Text)
		}
	}()

	return t
}

func touch(path string) {
	ioutil.WriteFile(path, nil, 0644)
}

func genTyrantFile(name, ext string) string {
	fname := "tyrant_" + name + "." + ext

	return fname
}

func (self *ShellExecutor) OnLaunchTask(driver *mesos.ExecutorDriver, taskInfo mesos.TaskInfo) {
	taskId := taskInfo.TaskId.GetValue()
	fmt.Println("Launch task:", taskId)
	log.Debug("send running state")
	self.sendStatusUpdate(taskId, mesos.TaskState_TASK_RUNNING, "task is running!")
	eventFile := genTyrantFile(taskId, "event")
	touch(eventFile)
	os.Setenv("TyrantStatusFile", eventFile)
	f := self.tailf(eventFile, taskId)

	log.Debugf("%+v", os.Args)
	startch := make(chan struct{}, 1)
	if len(os.Args) == 2 {
		fname := genTyrantFile(taskId, "sh")
		arg, err := base64.StdEncoding.DecodeString(os.Args[1])
		if err != nil {
			log.Error(err, arg)
		}
		ioutil.WriteFile(fname, arg, 0644)
		cmd := exec.Command("/bin/sh", fname)
		go func() {
			var err error
			defer func() {
				s := mesos.TaskState_TASK_FINISHED
				if err != nil {
					s = mesos.TaskState_TASK_FAILED
				}
				self.finish <- taskId
				log.Debug("send taskend state")
				self.sendStatusUpdate(taskId, s, "")
				f.Stop()
				time.Sleep(3 * time.Second)
				driver.Stop()
			}()

			self.lock.Lock()
			self.process[taskId] = &contex{cmd: cmd, statusFile: f}
			self.lock.Unlock()
			startch <- struct{}{}
			err = cmd.Start()
			if err != nil {
				log.Warning(err)
				return
			}
			log.Debug("pid", cmd.Process.Pid)
			err = cmd.Wait()
			if err != nil {
				log.Warning(err)
				return
			}
		}()
	} else {
		log.Debug("argc", len(os.Args), os.Args)
		log.Debug("send finish state")
		self.sendStatusUpdate(taskId, mesos.TaskState_TASK_FINISHED, "Go task is done!")
		time.Sleep(10 * time.Second)
		driver.Stop()
	}
	<-startch
}

func (self *ShellExecutor) OnShutdown(driver *mesos.ExecutorDriver) {
	log.Warning("shutdown executor")
}

func (self *ShellExecutor) OnError(driver *mesos.ExecutorDriver, errMsg string) {
	log.Warning(errMsg)
}

func (self *ShellExecutor) OnDisconnected(driver *mesos.ExecutorDriver) {
	log.Warning("disconnected")
}

func main() {
	log.SetHighlighting(false)
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	se := &ShellExecutor{pwd: pwd, finish: make(chan string),
		process: make(map[string]*contex)}
	driver := mesos.ExecutorDriver{
		Executor: &mesos.Executor{
			Registered:   se.OnRegister,
			KillTask:     se.OnKillTask,
			LaunchTask:   se.OnLaunchTask,
			Shutdown:     se.OnShutdown,
			Error:        se.OnError,
			Disconnected: se.OnDisconnected,
		},
	}

	go se.EventLoop()

	driver.Init()
	defer driver.Destroy()

	driver.Run()
}
