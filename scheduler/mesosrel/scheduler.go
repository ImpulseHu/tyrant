package mesosrel

import (
	"flag"
	"fmt"
	"strconv"
	"time"

	"github.com/juju/errors"
	log "github.com/ngaut/logging"

	"code.google.com/p/goprotobuf/proto"
	"github.com/ngaut/tyrant/scheduler"
	"mesos.apache.org/mesos"
)

type ResMan struct {
	exit        chan bool
	taskId      int
	timeoutSec  int
	cmdCh       chan interface{}
	running     *TaskQueue
	ready       *TaskQueue
	masterInfo  mesos.MasterInfo
	frameworkId string
	driver      *mesos.SchedulerDriver
}

type mesosDriver struct {
	driver *mesos.SchedulerDriver
	wait   chan struct{}
}

var (
	failoverTimeout = flag.Float64("failoverTimeout", 60, "failover timeout")
)

func NewResMan() *ResMan {
	return &ResMan{
		ready:      NewTaskQueue(),
		running:    NewTaskQueue(),
		exit:       make(chan bool),
		cmdCh:      make(chan interface{}, 1000),
		timeoutSec: 30,
	}
}

func (self *ResMan) OnStartReady(jid string) (string, error) {
	t := &cmdRunTask{Id: jid, ch: make(chan *pair, 1)}
	self.cmdCh <- t
	res := <-t.ch
	if len(res.a0.(string)) > 0 {
		return res.a0.(string), nil
	}

	return "", res.a1.(error)
}

func (self *ResMan) addReadyTask(id string) (string, error) {
	if self.ready.Exist(id) {
		return "", fmt.Errorf("%s already exist: %+v", id, self.ready.Get(id))
	}

	job, err := scheduler.GetJobById(id)
	if err != nil {
		return "", errors.Trace(err)
	}

	persistentTask := &scheduler.Task{TaskId: self.genTaskId(), Status: scheduler.STATUS_READY,
		StartTs: time.Now().Unix(), JobName: job.Name}
	log.Debugf("%+v", persistentTask)
	err = persistentTask.Save()
	if err != nil {
		return "", errors.Trace(err)
	}

	job.LastTaskId = persistentTask.TaskId
	job.Save()

	t := &Task{Tid: persistentTask.TaskId, job: job, state: taskReady}
	self.ready.Add(t.Tid, t)
	log.Debugf("ready task %+v, total count:%d", t, self.ready.Length())

	return persistentTask.TaskId, nil
}

func (self *ResMan) handleAddRunTask(t *cmdRunTask) {
	tid, err := self.addReadyTask(t.Id)
	if err != nil {
		log.Warning(err)
		t.ch <- &pair{a1: err}
		return
	}

	log.Debug("add task, taskId:", tid)

	t.ch <- &pair{a0: tid, a1: err}
}

func (self *ResMan) GetStatusByTaskId(taskId string) (string, error) {
	cmd := &cmdGetTaskStatus{taskId: taskId, ch: make(chan *pair)}
	self.cmdCh <- cmd
	res := <-cmd.ch
	if len(res.a0.(string)) > 0 {
		return res.a0.(string), nil
	}

	return "", res.a1.(error)
}

func (self *ResMan) handleMesosError(t *cmdMesosError) {
	defer func() {
		t.wait <- struct{}{}
	}()

	log.Errorf("%s\n", t.err)
}

func (self *ResMan) handleMesosOffers(t *cmdMesosOffers) {
	driver := t.driver
	offers := t.offers

	defer func() {
		t.wait <- struct{}{}
	}()

	log.Debug("ResourceOffers")
	ts := self.getReadyTasks()
	log.Debugf("ready tasks:%+v", ts)
	var idx, left int

	for idx = 0; idx < len(offers); idx++ {
		n := self.runTaskUsingOffer(driver, offers[idx], ts[left:])
		if n == 0 {
			break
		}
		left += n
	}

	//decline left offers
	for i := idx; i < len(offers); i++ {
		driver.DeclineOffer(offers[i].Id)
	}
}

func (self *ResMan) removeRunningTask(id string) {
	self.running.Del(id)
}

func (self *ResMan) handleMesosStatusUpdate(t *cmdMesosStatusUpdate) {
	status := t.status

	defer func() {
		t.wait <- struct{}{}
	}()

	taskId := *status.TaskId
	id := *taskId.Value
	log.Debugf("Received task %+v status: %+v", id, status)
	currentTask := self.running.Get(id)
	if currentTask == nil {
		return
	}

	//todo:check database and add this task to running queue
	pwd := string(status.Data)
	if len(pwd) > 0 && len(currentTask.Pwd) == 0 {
		currentTask.Pwd = pwd
	}

	currentTask.LastUpdate = time.Now()

	persistentTask, err := scheduler.GetTaskByTaskId(id)
	if err != nil {
		log.Error(err)
	}

	switch *status.State {
	case mesos.TaskState_TASK_FINISHED:
		currentTask.job.LastSuccessTs = time.Now().Unix()
		self.removeRunningTask(id)
	case mesos.TaskState_TASK_FAILED, mesos.TaskState_TASK_KILLED, mesos.TaskState_TASK_LOST:
		currentTask.job.LastErrTs = time.Now().Unix()
		self.removeRunningTask(id)
	case mesos.TaskState_TASK_STAGING:
		//todo: update something
	case mesos.TaskState_TASK_STARTING:
		//todo:update something
	case mesos.TaskState_TASK_RUNNING:
		//todo:update something
	default:
		log.Fatalf("should never happend %+v", status.State)
	}

	self.saveTaskStatus(persistentTask, status, currentTask)
}

func (self *ResMan) saveTaskStatus(persistentTask *scheduler.Task, status mesos.TaskStatus, currentTask *Task) {
	if persistentTask == nil {
		return
	}

	var url string
	if len(currentTask.Pwd) > 0 {
		url = fmt.Sprintf("http://%v:%v/#/slaves/%s/browse?path=%s",
			Inet_itoa(self.masterInfo.GetIp()), self.masterInfo.GetPort(), currentTask.SalveId, currentTask.Pwd)
	} else {
		url = fmt.Sprintf("http://%v:%v/#/frameworks/%s", Inet_itoa(self.masterInfo.GetIp()),
			self.masterInfo.GetPort(), self.frameworkId)
	}
	persistentTask.Status = (*status.State).String()
	if len(status.GetMessage()) > 0 {
		persistentTask.Message = status.GetMessage()
	}
	persistentTask.Url = url
	currentTask.job.LastStatus = persistentTask.Status
	currentTask.job.Save()
	persistentTask.UpdateTs = time.Now().Unix()
	persistentTask.Save()
	currentTask.job.SendNotify(persistentTask)
	log.Debugf("persistentTask:%+v", persistentTask)
}

func (self *ResMan) OnRunJob(id string) (string, error) {
	log.Debug("OnRunJob", id)
	cmd := &cmdRunTask{Id: id, ch: make(chan *pair, 1)}
	self.cmdCh <- cmd
	res := <-cmd.ch
	if len(res.a0.(string)) > 0 {
		return res.a0.(string), nil
	}

	return "", res.a1.(error)
}

func (self *ResMan) dispatch(cmd interface{}) {
	switch cmd.(type) {
	case *cmdRunTask:
		t := cmd.(*cmdRunTask)
		self.handleAddRunTask(t)
	case *cmdMesosError:
		t := cmd.(*cmdMesosError)
		self.handleMesosError(t)
	case *cmdMesosOffers:
		t := cmd.(*cmdMesosOffers)
		self.handleMesosOffers(t)
	case *cmdMesosStatusUpdate:
		t := cmd.(*cmdMesosStatusUpdate)
		self.handleMesosStatusUpdate(t)
	case *cmdMesosMasterInfoUpdate:
		info := cmd.(*cmdMesosMasterInfoUpdate)
		self.masterInfo = info.masterInfo
		self.driver = info.driver
		if len(*info.frameworkId.Value) > 0 {
			self.frameworkId = *info.frameworkId.Value
		}
	}
}

func (self *ResMan) TimeoutCheck(sec int) {
	var timeoutTasks []string
	self.running.Each(func(key string, t *Task) bool {
		if t.state == taskRuning && time.Since(t.LastUpdate).Seconds() > float64(sec) {
			log.Warningf("%+v timeout", t)
			timeoutTasks = append(timeoutTasks, key)
		}
		return true
	})

	for _, taskId := range timeoutTasks {
		log.Warningf("remove timeout task %s", taskId)
		mid := &mesos.TaskID{}
		id := taskId
		mid.Value = &id
		self.driver.KillTask(mid)
		//self.running.Del(taskId)
	}
}

func (self *ResMan) EventLoop() {
	tick := time.NewTicker(3 * time.Second)
	for {
		select {
		case cmd := <-self.cmdCh:
			self.dispatch(cmd)
		case <-tick.C:
			self.TimeoutCheck(self.timeoutSec)
		}
	}
}

func (self *ResMan) getReadyTasks() []*Task {
	var rts []*Task
	self.ready.Each(func(key string, t *Task) bool {
		log.Debugf("ready task:%+v", t)
		rts = append(rts, t)
		return true
	})

	log.Debugf("ready tasks: %+v", rts)

	return rts
}

func (self *ResMan) genExtorId(taskId string) string {
	return taskId
}

func (self *ResMan) genTaskId() string {
	self.taskId++
	return strconv.Itoa(int(time.Now().Unix())) + "_" + strconv.Itoa(self.taskId)
}

func (self *ResMan) runTaskUsingOffer(driver *mesos.SchedulerDriver, offer mesos.Offer,
	ts []*Task) (launchCount int) {
	cpus, mem := extraCpuMem(offer)
	var tasks []mesos.TaskInfo
	for i := 0; i < len(ts) && cpus > 0 && mem > 512; i++ {
		t := ts[i]
		log.Debugf("Launching task: %s\n", t.Tid)
		job := t.job
		executor := &mesos.ExecutorInfo{
			ExecutorId: &mesos.ExecutorID{Value: proto.String("default")},
			Command: &mesos.CommandInfo{
				Value: proto.String(""),
			},
			Name:   proto.String("shell executor (Go)"),
			Source: proto.String("go_test"),
		}

		executor.Command.Value = proto.String(job.Executor + ` "` + job.ExecutorFlags + `"`)
		executorId := self.genExtorId(t.Tid)
		executor.ExecutorId = &mesos.ExecutorID{Value: proto.String(executorId)}
		log.Debug(*executor.Command.Value)

		urls := splitTrim(job.Uris)
		taskUris := make([]*mesos.CommandInfo_URI, len(urls))
		for i, _ := range urls {
			taskUris[i] = &mesos.CommandInfo_URI{Value: &urls[i]}
		}
		executor.Command.Uris = taskUris

		task := mesos.TaskInfo{
			Name: proto.String("go-task"),
			TaskId: &mesos.TaskID{
				Value: proto.String(t.Tid),
			},
			SlaveId:  offer.SlaveId,
			Executor: executor,
			Resources: []*mesos.Resource{
				mesos.ScalarResource("cpus", 1),
				mesos.ScalarResource("mem", 512),
			},
		}

		tasks = append(tasks, task)
		t.state = taskRuning

		t.LastUpdate = time.Now()
		t.SalveId = offer.GetSlaveId().GetValue()
		t.OfferId = offer.GetId().GetValue()
		log.Warning(t.OfferId)
		t.ExecutorId = executorId
		self.running.Add(t.Tid, t)
		log.Debugf("remove %+v from ready queue", t.Tid)
		self.ready.Del(t.Tid)
		cpus -= CPU_UNIT
		mem -= MEM_UNIT
	}

	if len(tasks) == 0 {
		return 0
	}

	log.Debugf("%+v", tasks)

	driver.LaunchTasks(offer.Id, tasks)

	return len(tasks)
}

func (self *ResMan) OnResourceOffers(driver *mesos.SchedulerDriver, offers []mesos.Offer) {
	cmd := &cmdMesosOffers{
		mesosDriver: mesosDriver{
			driver: driver,
			wait:   make(chan struct{}),
		},
		offers: offers,
	}

	self.cmdCh <- cmd
	<-cmd.wait
}

func (self *ResMan) OnStatusUpdate(driver *mesos.SchedulerDriver, status mesos.TaskStatus) {
	cmd := &cmdMesosStatusUpdate{
		mesosDriver: mesosDriver{
			driver: driver,
			wait:   make(chan struct{}),
		},
		status: status,
	}

	self.cmdCh <- cmd
	<-cmd.wait

}

func (self *ResMan) OnError(driver *mesos.SchedulerDriver, err string) {
	cmd := &cmdMesosError{
		mesosDriver: mesosDriver{
			driver: driver,
			wait:   make(chan struct{}),
		},
		err: err,
	}

	self.cmdCh <- cmd
	<-cmd.wait

}

func (self *ResMan) OnDisconnected(driver *mesos.SchedulerDriver) {
	log.Warning("Disconnected")
}

func (self *ResMan) OnRegister(driver *mesos.SchedulerDriver, fid mesos.FrameworkID, mi mesos.MasterInfo) {
	log.Warningf("OnRegisterd master:%v:%v, frameworkId:%v", Inet_itoa(mi.GetIp()), mi.GetPort(), fid.GetValue())
	cmd := &cmdMesosMasterInfoUpdate{masterInfo: mi, frameworkId: fid, driver: driver}
	self.cmdCh <- cmd
}

func (self *ResMan) OnReregister(driver *mesos.SchedulerDriver, mi mesos.MasterInfo) {
	log.Warningf("OnReregisterd master:%v:%v", Inet_itoa(mi.GetIp()), mi.GetPort())
	cmd := &cmdMesosMasterInfoUpdate{masterInfo: mi, driver: driver}
	self.cmdCh <- cmd
}

func (self *ResMan) Run(master string) {
	frameworkIdStr := FRAMEWORK_ID
	frameworkId := &mesos.FrameworkID{Value: &frameworkIdStr}
	driver := mesos.SchedulerDriver{
		Master: master,
		Framework: mesos.FrameworkInfo{
			Name:            proto.String("GoFramework"),
			User:            proto.String(""),
			FailoverTimeout: failoverTimeout,
			Id:              frameworkId,
		},

		Scheduler: &mesos.Scheduler{
			ResourceOffers: self.OnResourceOffers,
			StatusUpdate:   self.OnStatusUpdate,
			Error:          self.OnError,
			Disconnected:   self.OnDisconnected,
			Registered:     self.OnRegister,
			Reregistered:   self.OnReregister,
		},
	}

	driver.Init()
	defer driver.Destroy()
	go self.EventLoop()

	driver.Start()
	<-self.exit
	log.Debug("exit")
	driver.Stop(false)
}
