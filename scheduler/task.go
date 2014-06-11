package scheduler

import log "github.com/ngaut/logging"

type Task struct {
	Id       int64  `db:"auto_incr_id" json:"auto_incr_id"`
	TaskId   string `db:"id" json:"id"`
	JobName  string `db:"job_name" json:"job_name"`
	Status   string `db:"status" json:"status"` /* `READY -> RUNNING -> (FINISH | FAILED)`*/
	Message  string `db:"message" json:"message"`
	Url      string `db:"url" json:"url"`
	StartTs  int64  `db:"start_ts" json:"start_ts"`
	UpdateTs int64  `db:"update_ts" json:"update_ts"`
}

var STATUS_READY string = "READY"
var STATUS_RUNNING string = "RUNNING"
var STATUS_SUCCESS string = "SUCCESS"
var STATUS_FAILED string = "FAILED"

func GetTaskList() []Task {
	var tasks []Task
	_, err := sharedDbMap.Select(&tasks, "select * from tasks order by start_ts desc")
	if err != nil {
		log.Debug(err.Error())
		return nil
	}
	return tasks
}

func GetTaskByTaskId(id string) (*Task, error) {
	var task Task
	err := sharedDbMap.SelectOne(&task, "select * from tasks where id=?", id)
	if err != nil {
		log.Debug(err.Error())
		return nil, err
	}
	return &task, nil
}

func (t *Task) Save() error {
	if t.Id == 0 {
		return sharedDbMap.Insert(t)
	} else {
		_, err := sharedDbMap.Update(t)
		return err
	}
}
