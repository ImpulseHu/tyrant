package scheduler

import (
	log "github.com/ngaut/logging"
	"strconv"
)

type Task struct {
	TaskAutoId int64  `db:"auto_task_id"`
	Id         string `db:"id" json:"id"`
	JobName    string `db:"job_name" json:"job_name"`
	Status     string `db:"status" json:"status"` /* `READY -> RUNNING -> (FINISH | FAILED)`*/
	Message    string `db:"message" json:"message"`
	Url        string `db:"url" json:"url"`
	StartTs    int64  `db:"start_ts" json:"start_ts"`
	UpdateTs   int64  `db:"update_ts" json:"update_ts"`
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

func GetTaskById(id string) (*Task, error) {
	nid, err := strconv.Atoi(id)
	if err != nil {
		log.Debug(err.Error())
		return nil, err
	}
	var task Task
	err = sharedDbMap.SelectOne(&task, "select * from tasks where id=?", nid)
	if err != nil {
		log.Debug(err.Error())
		return nil, err
	}
	return &task, nil
}

func (j *Task) Save() error {
	if j.TaskAutoId == 0 {
		return sharedDbMap.Insert(j)
	} else {
		_, err := sharedDbMap.Update(j)
		return err
	}
}
