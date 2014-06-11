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
	Id          string
	job         *scheduler.Job
	state       int
	Details     string
	FrameworkId string
	SalveId     string
	ExecutorId  string
	LastUpdate  time.Time
}

func statusToStr(status int) string {
	return statusMap[status]
}

func (self *Task) Status() (string, string) {
	return statusToStr(self.state), self.Details
}

func (self *Task) String() string {
	return fmt.Sprintf(
		"Id:%s, job:%+v, state:%s, Details:%s, LastUpdate:%v",
		self.Id, self.job, statusToStr(self.state), self.Details, self.LastUpdate,
	)
}
