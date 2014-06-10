package resourceScheduler

import (
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
	Id         string
	job        *scheduler.Job
	state      int
	Details    string
	LastUpdate time.Time
}

func statusToStr(status int) string {
	return statusMap[status]
}

func (self *Task) Status() (string, string) {
	return statusToStr(self.state), self.Details
}
