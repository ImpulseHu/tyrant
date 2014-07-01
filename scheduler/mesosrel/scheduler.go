package mesosrel

import (
	"encoding/base64"
	"flag"
	"fmt"
	"strconv"
	"time"

	"github.com/juju/errors"
	log "github.com/ngaut/logging"

	"code.google.com/p/goprotobuf/proto"
	"github.com/ngaut/tyrant/notify"
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
	notifier    *notify.Notifier
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
		cmdCh:      make(chan interface{}, 10),
		timeoutSec: 30,
		notifier:   notify.NewNotifier(),
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

	log.Debugf("ResourceOffers %+v", offers)
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

	taskId := status.TaskId.GetValue()
	log.Debugf("Received task %+v status: %+v", taskId, status)
	currentTask := self.running.Get(taskId)
	if currentTask == nil {
		task, err := scheduler.GetTaskByTaskId(taskId)
		if err != nil {
			return
		}
		job, err := scheduler.GetJobByName(task.JobName)
		if err != nil {
			return
		}
		currentTask = &Task{Tid: task.TaskId, job: job, SlaveId: status.SlaveId.GetValue(), state: taskRuning}
		self.running.Add(currentTask.Tid, currentTask) //add this alone task to runing queue
	}

	pwd := string(status.Data)
	if len(pwd) > 0 && len(currentTask.Pwd) == 0 {
		currentTask.Pwd = pwd
	}

	currentTask.LastUpdate = time.Now()

	switch *status.State {
	case mesos.TaskState_TASK_FINISHED:
		currentTask.job.LastSuccessTs = time.Now().Unix()
		self.removeRunningTask(taskId)
	case mesos.TaskState_TASK_FAILED, mesos.TaskState_TASK_KILLED, mesos.TaskState_TASK_LOST:
		currentTask.job.LastErrTs = time.Now().Unix()
		self.removeRunningTask(taskId)
	case mesos.TaskState_TASK_STAGING:
		//todo: update something
	case mesos.TaskState_TASK_STARTING:
		//todo:update something
	case mesos.TaskState_TASK_RUNNING:
		//todo:update something
	default:
		log.Fatalf("should never happend %+v", status.State)
	}

	persistentTask, err := scheduler.GetTaskByTaskId(taskId)
	if err != nil {
		log.Error(err)
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
			Inet_itoa(self.masterInfo.GetIp()), self.masterInfo.GetPort(), currentTask.SlaveId, currentTask.Pwd)
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
	switch *status.State {
	case mesos.TaskState_TASK_FINISHED, mesos.TaskState_TASK_FAILED,
		mesos.TaskState_TASK_KILLED, mesos.TaskState_TASK_LOST:
		self.notifier.SendNotify(currentTask.job, persistentTask, true)
	default:
		self.notifier.SendNotify(currentTask.job, persistentTask, false)
	}

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

func (self *ResMan) OnKillTask(taskId string) error {
	log.Warning("KillTask", taskId)
	cmd := &cmdKillTask{taskId: taskId, ch: make(chan *pair, 1)}
	self.cmdCh <- cmd
	res := <-cmd.ch
	if len(res.a0.(string)) > 0 {
		return nil
	}

	return res.a1.(error)
}

func (self *ResMan) handleMesosMasterInfoUpdate(info *cmdMesosMasterInfoUpdate) {
	self.masterInfo = info.masterInfo
	self.driver = info.driver
	if len(*info.frameworkId.Value) > 0 {
		self.frameworkId = *info.frameworkId.Value
	}
}
func (self *ResMan) dispatch(cmd interface{}) {
	switch cmd.(type) {
	case *cmdRunTask:
		self.handleAddRunTask(cmd.(*cmdRunTask))
	case *cmdMesosError:
		self.handleMesosError(cmd.(*cmdMesosError))
	case *cmdMesosOffers:
		self.handleMesosOffers(cmd.(*cmdMesosOffers))
	case *cmdMesosStatusUpdate:
		self.handleMesosStatusUpdate(cmd.(*cmdMesosStatusUpdate))
	case *cmdMesosMasterInfoUpdate:
		info := cmd.(*cmdMesosMasterInfoUpdate)
		self.handleMesosMasterInfoUpdate(info)
	case *cmdKillTask:
		info := cmd.(*cmdKillTask)
		task := self.running.Get(info.taskId)
		if task != nil {
			self.driver.KillTask(&mesos.TaskID{Value: &info.taskId})
			info.ch <- &pair{a0: "ok"}
		} else {
			info.ch <- &pair{a1: errors.New("task not running")}
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
		//todo: policy support
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
	for i := 0; i < len(ts) && cpus > CPU_UNIT && mem > MEM_UNIT; i++ {
		t := ts[i]
		log.Debugf("Launching task: %s\n", t.Tid)
		job := t.job
		executor := &mesos.ExecutorInfo{
			Command: &mesos.CommandInfo{
				//Value: proto.String(job.Executor + ` "` + job.ExecutorFlags + `"`),
				Value: proto.String(fmt.Sprintf(`%s "%s"`, job.Executor,
					base64.StdEncoding.EncodeToString([]byte(job.ExecutorFlags)))),
			},
			Name:   proto.String("shell executor (Go)"),
			Source: proto.String("go_test"),
		}

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
				mesos.ScalarResource("cpus", CPU_UNIT),
				mesos.ScalarResource("mem", MEM_UNIT),
			},
		}

		tasks = append(tasks, task)
		t.state = taskRuning

		t.LastUpdate = time.Now()
		t.SlaveId = offer.GetSlaveId().GetValue()
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
			Name:            proto.String("TyrantFramework"),
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
