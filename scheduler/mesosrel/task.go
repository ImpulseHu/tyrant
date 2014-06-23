package mesosrel

import (
	"fmt"
	"time"

	"github.com/ngaut/tyrant/scheduler"
)

const (
	taskReady  = 1
	taskRuning = 2
)

var (
	statusMap = map[int]string{
		taskReady:  "ready",
		taskRuning: "running",
	}
)

type Task struct {
	Tid         string
	job         *scheduler.Job
	state       int
	FrameworkId string
	SlaveId     string
	Pwd         string
	LastUpdate  time.Time
}

func statusToStr(status int) string {
	return statusMap[status]
}

func (self *Task) Status() string {
	return statusToStr(self.state)
}

func (self *Task) String() string {
	return fmt.Sprintf(
		"Id:%s, job:%+v, state:%s, LastUpdate:%v",
		self.Tid, self.job, statusToStr(self.state), self.LastUpdate,
	)
}
