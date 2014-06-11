package mesosrel

import (
	"fmt"
	"strings"

	"mesos.apache.org/mesos"
)

type cmdRunTask struct {
	Id string
	ch chan *pair //return task id and error
}

type cmdGetTaskStatus struct {
	taskId string
	ch     chan *pair
}

//frame registered or reregistered
type cmdMesosMasterInfoUpdate struct {
	masterInfo  mesos.MasterInfo
	frameworkId mesos.FrameworkID
	ch          chan struct{}
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

func Inet_itoa(a uint32) string {
	return fmt.Sprintf("%d.%d.%d.%d", byte(a), byte(a>>8), byte(a>>16), byte(a>>24))
}

type pair struct {
	a0 interface{}
	a1 interface{}
}

type TaskQueue struct {
	tasks map[string]*Task
}

func NewTaskQueue() *TaskQueue {
	return &TaskQueue{tasks: make(map[string]*Task)}
}

func (self *TaskQueue) Get(key string) *Task {
	return self.tasks[key]
}

func (self *TaskQueue) Add(key string, task *Task) {
	self.tasks[key] = task
}

func (self *TaskQueue) Del(key string) {
	delete(self.tasks, key)
}

func (self *TaskQueue) Exist(key string) bool {
	_, ok := self.tasks[key]
	return ok
}

func (self *TaskQueue) Each(f func(string, *Task) bool) {
	for k, task := range self.tasks {
		if !f(k, task) {
			break
		}
	}
}

func (self *TaskQueue) Length() int {
	return len(self.tasks)
}
