package notify

import (
	"bytes"
	"encoding/json"
	"net/http"

	log "github.com/ngaut/logging"
	"github.com/ngaut/tyrant/scheduler"
)

type WebhookInfo struct {
	job    *scheduler.Job
	task   *scheduler.Task
	isLast bool //last update message
}

type taskNotify struct {
	ch chan *WebhookInfo
}

type Notifier struct {
	updateMsgQ chan *WebhookInfo
	tasks      map[string]*taskNotify
}

func (self *taskNotify) run() {
	for msg := range self.ch {
		//call webhook
		log.Debug("Send Notify for Job", msg.job, msg.task)
		if len(msg.job.WebHookUrl) == 0 {
			continue
		}
		buf, err := json.Marshal(struct {
			Job  *scheduler.Job  `json:"job"`
			Task *scheduler.Task `json:"task"`
		}{msg.job, msg.task})
		if err != nil {
			log.Warning(err.Error(), msg.job, msg.task)
		}
		body := bytes.NewBuffer(buf)
		_, err = http.Post(msg.job.WebHookUrl, "application/json", body)
		if err != nil {
			log.Warning(err.Error(), msg.job, msg.task)
		}

		if msg.isLast { //no more message
			return
		}
	}
}

func NewNotifier() *Notifier {
	n := &Notifier{
		updateMsgQ: make(chan *WebhookInfo, 10),
		tasks:      make(map[string]*taskNotify),
	}

	go n.EventLoop()

	return n
}

func (self *Notifier) SendNotify(job *scheduler.Job, task *scheduler.Task, isLast bool) {
	self.updateMsgQ <- &WebhookInfo{job: job, task: task, isLast: isLast}
}

func (self *Notifier) EventLoop() {
	for {
		select {
		case msg := <-self.updateMsgQ:
			taskId := msg.task.TaskId
			n, ok := self.tasks[taskId]
			if !ok {
				n = &taskNotify{ch: make(chan *WebhookInfo, 100)}
				self.tasks[taskId] = n
				go n.run()
			}
			n.ch <- msg
			if msg.isLast {
				delete(self.tasks, taskId)
			}
		}
	}
}
