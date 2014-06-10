package resourceScheduler

import (
	"encoding/base64"
	"encoding/json"
	"strings"

	log "github.com/ngaut/logging"
)

type TyrantTaskId struct {
	Id       string
	TaskName string
}

type cmdRunTask struct {
	Id string
	ch chan *pair //return task id and error
}

type cmdGetTaskStatus struct {
	taskId string
	ch     chan *pair
}

func genTaskId(jid string, taskName string) string {
	if buf, err := json.Marshal(TyrantTaskId{Id: jid, TaskName: taskName}); err != nil {
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
