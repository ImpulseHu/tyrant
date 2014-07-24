package mesosrel

import (
	"github.com/mesosphere/mesos-go/mesos"
	"github.com/ngaut/tyrant/scheduler"
)

const (
	CPU_UNIT     = 1
	MEM_UNIT     = 512
	FRAMEWORK_ID = "tryant"
)

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

type cmdRunTask struct {
	job *scheduler.Job
	ch  chan *pair //return task id and error
}

type cmdGetTaskStatus struct {
	taskId string
	ch     chan *pair
}

//frame registered or reregistered
type cmdMesosMasterInfoUpdate struct {
	masterInfo  mesos.MasterInfo
	driver      *mesos.SchedulerDriver
	frameworkId mesos.FrameworkID
	ch          chan struct{}
}

type cmdKillTask struct {
	taskId string
	ch     chan *pair
}
