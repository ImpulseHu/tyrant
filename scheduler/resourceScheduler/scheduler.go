package resourceScheduler

import (
	"flag"
	"fmt"
	"strconv"
	"time"

	log "github.com/ngaut/logging"

	"code.google.com/p/goprotobuf/proto"
	"github.com/ngaut/tyrant/scheduler"
	"mesos.apache.org/mesos"
)

type ResMan struct {
	executor   *mesos.ExecutorInfo
	exit       chan bool
	taskId     int
	timeoutSec int
	cmdCh      chan interface{}
	running    *TaskQueue
	ready      *TaskQueue
}

type mesosDriver struct {
	driver *mesos.SchedulerDriver
	wait   chan struct{}
}

type cmdMesosOffers struct {
	mesosDriver
	offers []mesos.Offer
}

type cmdMesosError struct {
	mesosDriver
	err string
}

type cmdMesosStatusUpdate struct {
	mesosDriver
	status mesos.TaskStatus
}

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

func (self *ResMan) addTask(id string) error {
	if self.ready.Exist(id) {
		return fmt.Errorf("%s already exist: %+v", id, self.ready.Get(id))
	}

	job, err := scheduler.GetJobById(id)
	if err != nil {
		return err
	}

	self.ready.Add(id, &Task{job: job, state: taskReady})
	return nil
}

func (self *ResMan) handleAddRunTask(t *cmdRunTask) {
	err := self.addTask(t.Id)
	if err != nil {
		t.ch <- &pair{a1: err}
		return
	}

	t.ch <- &pair{a0: t.Id, a1: err}
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

	//remove from ready queue
	for i := 0; i < idx; i++ {
		self.ready.Del(ts[i].Id)
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
	ti := decodeTaskId(*taskId.Value)
	log.Debugf("Received task %+v status: %+v", ti, status)
	id := ti.Id
	j := self.running.Get(id)
	if j != nil {
		return
	}

	j.Details = status.GetMessage()
	j.LastUpdate = time.Now()

	//todo: update in storage
	switch *status.State {
	case mesos.TaskState_TASK_FINISHED:
		self.removeRunningTask(id)
	case mesos.TaskState_TASK_FAILED:
		self.removeRunningTask(id)
	case mesos.TaskState_TASK_KILLED:
		self.removeRunningTask(id)
	case mesos.TaskState_TASK_LOST:
		self.removeRunningTask(id)
	case mesos.TaskState_TASK_STAGING:
		//todo: update something
	case mesos.TaskState_TASK_STARTING:
		//todo:update something
	case mesos.TaskState_TASK_RUNNING:
		//todo:update something
	default:
		panic("should never happend")
	}
}

func (self *ResMan) OnRunJob(id string) (string, error) {
	cmd := &cmdRunTask{Id: id, ch: make(chan *pair, 1)}
	self.cmdCh <- cmd
	res := <-cmd.ch
	if len(res.a0.(string)) > 0 {
		return res.a0.(string), nil
	}

	return "", res.a1.(error)
}

func (self *ResMan) handleCmd(cmd interface{}) {
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
	case *cmdGetTaskStatus:
		ts := cmd.(*cmdGetTaskStatus)
		t := self.running.Get(ts.taskId)
		if t != nil {
			ts.ch <- &pair{a1: fmt.Errorf("%s not exist", ts.taskId)}
			return
		}
		a0, a1 := t.Status()
		ts.ch <- &pair{a0: a0, a1: a1}
	}
}

func (self *ResMan) TimeoutCheck(sec int) {
	var timeoutTasks []string
	self.running.Each(func(key string, t *Task) bool {
		if t.state == taskRuning && time.Since(t.LastUpdate).Seconds() > float64(sec) {
			log.Warning("%+v timeout", t)
			timeoutTasks = append(timeoutTasks, key)
		}
		return true
	})

	for _, taskId := range timeoutTasks {
		self.running.Del(taskId)
	}
}

func (self *ResMan) EventLoop() {
	tick := time.NewTicker(3 * time.Second)
	for {
		select {
		case cmd := <-self.cmdCh:
			self.handleCmd(cmd)
		case <-tick.C:
			self.TimeoutCheck(self.timeoutSec)
		}
	}
}

func (self *ResMan) getReadyTasks() []*Task {
	//todo:check if schedule time is match
	var rts []*Task
	self.running.Each(func(key string, t *Task) bool {
		rts = append(rts, t)
		return true
	})

	return rts
}

func extraCpuMem(offer mesos.Offer) (int, int) {
	var cpus int
	var mem int

	for _, r := range offer.Resources {
		if r.GetName() == "cpus" && r.GetType() == mesos.Value_SCALAR {
			cpus += int(r.GetScalar().GetValue())
		}

		if r.GetName() == "mem" && r.GetType() == mesos.Value_SCALAR {
			mem += int(r.GetScalar().GetValue())
		}
	}

	return cpus, mem
}

func (self *ResMan) runTaskUsingOffer(driver *mesos.SchedulerDriver, offer mesos.Offer,
	ts []*Task) (launchCount int) {
	cpus, mem := extraCpuMem(offer)
	tasks := make([]mesos.TaskInfo, 0)
	for i := 0; i < len(ts) && cpus > 0 && mem > 512; i++ {
		t := ts[i]
		self.taskId++
		log.Debugf("Launching task: %d, name:%s\n", self.taskId, t.Id)
		job := t.job

		self.executor.Command.Value = proto.String(job.Executor + ` "` + job.ExecutorFlags + `"`)
		self.executor.ExecutorId = &mesos.ExecutorID{Value: proto.String("tyrantExecutorId_" + strconv.Itoa(self.taskId))}
		log.Debug(*self.executor.Command.Value)

		urls := splitTrim(job.Uris)
		taskUris := make([]*mesos.CommandInfo_URI, len(urls))
		for i, _ := range urls {
			taskUris[i] = &mesos.CommandInfo_URI{Value: &urls[i]}
		}
		self.executor.Command.Uris = taskUris

		task := mesos.TaskInfo{
			Name: proto.String("go-task"),
			TaskId: &mesos.TaskID{
				Value: proto.String(genTaskId(strconv.Itoa(int(job.Id)), t.Id)),
			},
			SlaveId:  offer.SlaveId,
			Executor: self.executor,
			Resources: []*mesos.Resource{
				mesos.ScalarResource("cpus", 1),
				mesos.ScalarResource("mem", 512),
			},
		}

		tasks = append(tasks, task)
		t.state = taskRuning

		t.LastUpdate = time.Now()
	}

	if len(tasks) == 0 {
		return 0
	}

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

func (self *ResMan) Run() {
	master := flag.String("master", "localhost:5050", "Location of leading Mesos master")
	executorUri := flag.String("executor-uri", "", "URI of executor executable")
	flag.Parse()

	self.executor = &mesos.ExecutorInfo{
		ExecutorId: &mesos.ExecutorID{Value: proto.String("default")},
		Command: &mesos.CommandInfo{
			Value: proto.String("./example_executor"),
			Uris: []*mesos.CommandInfo_URI{
				&mesos.CommandInfo_URI{Value: executorUri},
			},
		},
		Name:   proto.String("Test Executor (Go)"),
		Source: proto.String("go_test"),
	}

	driver := mesos.SchedulerDriver{
		Master: *master,
		Framework: mesos.FrameworkInfo{
			Name: proto.String("GoFramework"),
			User: proto.String(""),
		},

		Scheduler: &mesos.Scheduler{
			ResourceOffers: self.OnResourceOffers,
			StatusUpdate:   self.OnStatusUpdate,
			Error:          self.OnError,
			Disconnected:   self.OnDisconnected,
		},
	}

	driver.Init()
	defer driver.Destroy()
	go self.EventLoop()

	driver.Start()
	<-self.exit
	driver.Stop(false)
}
