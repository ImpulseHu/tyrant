package resourceScheduler

/*
import (
	"flag"
	"fmt"
	"time"

	"strconv"

	log "github.com/ngaut/logging"

	"code.google.com/p/goprotobuf/proto"
	"github.com/ngaut/tyrant/scheduler"
	"mesos.apache.org/mesos"
)

type ResMan struct {
	s          *scheduler.TaskScheduler
	executor   *mesos.ExecutorInfo
	exit       chan bool
	taskId     int
	timeoutSec int
	cmdCh      chan interface{}
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
	return &ResMan{s: scheduler.NewTaskScheduler(), exit: make(chan bool),
		cmdCh:      make(chan interface{}, 1000),
		timeoutSec: 30,
	}
}

type ReadyTask struct {
	*scheduler.Task
	td *scheduler.TaskDag
}

func (self *ReadyTask) String() string {
	return self.td.DagName + ":  " + fmt.Sprintf("%+v", self.Task)
}

func (self *ResMan) OnStartReady(dagName string) (string, error) {
	t := &cmdRunTask{dagName: dagName, ch: make(chan *pair, 1)}
	self.cmdCh <- t
	res := <-t.ch
	if len(res.a0.(string)) > 0 {
		return res.a0.(string), nil
	}

	return "", res.a1.(error)
}

func (self *ResMan) handleAddRunTask(t *cmdRunTask) {
	_, err := self.s.TryAddTaskDag(t.dagName)
	if err != nil {
		t.ch <- &pair{a1: err}
		return
	}

	t.ch <- &pair{a0: t.dagName, a1: err}
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

func (self *ResMan) handleMesosStatusUpdate(t *cmdMesosStatusUpdate) {
	status := t.status

	defer func() {
		t.wait <- struct{}{}
	}()

	taskId := *status.TaskId
	ti := decodeTaskId(*taskId.Value)
	log.Debugf("Received task %+v status: %+v", ti, status)
	td := self.s.GetTaskDag(ti.DagName)
	if td == nil {
		return
	}

	td.Details = status.GetMessage()
	td.LastUpdate = time.Now()

	//todo: update in storage

	switch *status.State {
	case mesos.TaskState_TASK_FINISHED:
		self.removeTask(ti)
	case mesos.TaskState_TASK_FAILED:
		//self.removeTask(ti)
		self.s.RemoveTaskDag(td.DagName)
	case mesos.TaskState_TASK_KILLED:
		//self.removeTask(ti)
		self.s.RemoveTaskDag(td.DagName)
	case mesos.TaskState_TASK_LOST:
		//self.removeTask(ti)
		self.s.RemoveTaskDag(td.DagName)
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

func (self *ResMan) OnRunJob(name string) (string, error) {
	cmd := &cmdRunTask{dagName: name, ch: make(chan *pair, 1)}
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
		t := cmd.(*cmdGetTaskStatus)
		td := self.s.GetTaskDag(t.taskId)
		if td == nil {
			t.ch <- &pair{a1: fmt.Errorf("%s not exist", t.taskId)}
			return
		}
		a0, a1 := td.Status()
		t.ch <- &pair{a0: a0, a1: a1}
	}
}

func (self *ResMan) EventLoop() {
	tick := time.NewTicker(3 * time.Second)
	for {
		select {
		case cmd := <-self.cmdCh:
			self.handleCmd(cmd)
		case <-tick.C:
			self.s.TimeoutCheck(self.timeoutSec)
		}
	}
}

func (self *ResMan) getReadyTasks() []*ReadyTask {
	tds := self.s.GetReadyDags()
	log.Debugf("ready dag: %+v", tds)
	rts := make([]*ReadyTask, 0)
	//todo:check if schedule time is match
	for _, td := range tds {
		tasks := td.GetReadyTask()
		for _, t := range tasks {
			rts = append(rts, &ReadyTask{Task: t, td: td})
		}
	}

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
	ts []*ReadyTask) (launchCount int) {
	cpus, mem := extraCpuMem(offer)
	tasks := make([]mesos.TaskInfo, 0)
	for i := 0; i < len(ts) && cpus > 0 && mem > 512; i++ {
		t := ts[i]
		self.taskId++
		log.Debugf("Launching task: %d, name:%s\n", self.taskId, t.Name)
		job, err := scheduler.GetJobByName(t.Name)
		if err != nil {
			log.Error(err)
			driver.DeclineOffer(offer.Id)
			return
		}

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
				Value: proto.String(genTaskId(t.td.DagName, t.Name)),
			},
			SlaveId:  offer.SlaveId,
			Executor: self.executor,
			Resources: []*mesos.Resource{
				mesos.ScalarResource("cpus", 1),
				mesos.ScalarResource("mem", 512),
			},
		}

		tasks = append(tasks, task)

		self.s.SetTaskDagStateRunning(t.td.DagName)
		t.td.LastUpdate = time.Now()
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

func (self *ResMan) removeTask(ti *TyrantTaskId) {
	td := self.s.GetTaskDag(ti.DagName)
	td.RemoveTask(ti.TaskName)
	self.s.SetTaskDagStateReady(ti.DagName)

	if td.Dag.Empty() {
		log.Debugf("task in dag %s is empty, remove it", td.DagName)
		self.s.RemoveTaskDag(td.DagName)
		return
	}
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
*/
