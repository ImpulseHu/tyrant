package resourceScheduler

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"

	"strconv"
	"strings"

	log "github.com/ngaut/logging"

	"code.google.com/p/goprotobuf/proto"
	"github.com/ngaut/tyrant/scheduler"
	"mesos.apache.org/mesos"
)

type ResMan struct {
	s        *scheduler.TaskScheduler
	executor *mesos.ExecutorInfo
	exit     chan bool
	taskId   int
}

func NewResMan() *ResMan {
	return &ResMan{s: scheduler.NewTaskScheduler(), exit: make(chan bool)}
}

type TyrantTaskId struct {
	DagName  string
	TaskName string
}

func genTaskId(dagName string, taskName string) string {
	if buf, err := json.Marshal(TyrantTaskId{DagName: dagName, TaskName: taskName}); err != nil {
		log.Fatal(err)
	} else {
		return base64.StdEncoding.EncodeToString(buf)
	}

	return ""
}

func decodeTaskId(str string) *TyrantTaskId {
	data, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		log.Fatal(err)
	}

	var ti TyrantTaskId
	err = json.Unmarshal(data, &ti)
	if err != nil {
		log.Fatal(err)
	}

	return &ti
}

func splitTrim(s string) []string {
	tmp := strings.Split(s, ",")
	ss := make([]string, 0)
	for _, v := range tmp {
		if x := strings.Trim(v, " "); len(x) > 0 {
			ss = append(ss, x)
		}
	}

	return ss
}

type ReadyTask struct {
	*scheduler.Task
	td *scheduler.TaskDag
}

func (self *ReadyTask) String() string {
	return self.td.DagName + ":  " + fmt.Sprintf("%+v", self.Task)
}

func (self *ResMan) getReadyTasks() []*ReadyTask {
	tds := self.s.GetReadyDag()
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

func (self *ResMan) runTaskUsingOffer(driver *mesos.SchedulerDriver, offer mesos.Offer,
	ts []*ReadyTask) (launchCount int) {
	for i, t := range ts {
		self.taskId++
		log.Debugf("Launching task: %d, name:%s\n", self.taskId, t.Name)
		job, err := scheduler.GetJobByName(ts[i].Name)
		if err != nil {
			log.Error(err)
			driver.DeclineOffer(offer.Id)
			return
		}

		self.executor.Command.Value = proto.String(job.Command)
		self.executor.ExecutorId = &mesos.ExecutorID{Value: proto.String("tyrantExecutorId_" + strconv.Itoa(self.taskId))}
		log.Debug(job.Command, *self.executor.Command.Value)

		urls := splitTrim(job.Uris)
		taskUris := make([]*mesos.CommandInfo_URI, len(urls))
		for i, _ := range urls {
			taskUris[i] = &mesos.CommandInfo_URI{Value: &urls[i]}
		}
		self.executor.Command.Uris = taskUris

		tasks := []mesos.TaskInfo{
			mesos.TaskInfo{
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
			},
		}

		self.s.SetTaskDagStateRunning(t.td.DagName)

		driver.LaunchTasks(offer.Id, tasks)
		launchCount++
	}

	return
}

func (self *ResMan) OnResourceOffers(driver *mesos.SchedulerDriver, offers []mesos.Offer) {
	log.Debug("ResourceOffers")
	self.s.Refresh()
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
	taskId := *status.TaskId
	ti := decodeTaskId(*taskId.Value)
	log.Debugf("Received task %+v status: %+v", ti, status)
	switch *status.State {
	case mesos.TaskState_TASK_FINISHED:
		self.removeTask(ti)
	case mesos.TaskState_TASK_FAILED:
		//todo: retry
		self.removeTask(ti)
	case mesos.TaskState_TASK_KILLED:
		//todo:
		self.removeTask(ti)
	case mesos.TaskState_TASK_LOST:
		//todo:
		self.removeTask(ti)
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

func (self *ResMan) OnError(driver *mesos.SchedulerDriver, err string) {
	log.Errorf("%s\n", err)
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

	driver.Start()
	<-self.exit
	driver.Stop(false)
}
