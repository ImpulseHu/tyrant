package scheduler

import (
	"fmt"
	"strings"

	log "github.com/ngaut/logging"
)

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

func GetTotalTaskCount() (int64, error) {
	cnt, err := sharedDbMap.SelectInt("select count(*) from tasks")
	if err != nil {
		return -1, err
	}
	return cnt, nil
}

func GetTaskList() []Task {
	return GetTaskListWithOffsetAndFilter(-1, -1, nil)
}

func GetTaskListWithOffsetAndFilter(offset int, limit int, filter FilterInfo) []Task {
	var tasks []Task
	var filterFields []string
	sql := ""
	// create where statement
	for k, v := range filter {
		filterFields = append(filterFields, k+`="`+v+`"`)
	}
	var filterStr string
	if len(filterFields) > 0 {
		filterStr = strings.Join(filterFields, " and ")
		filterStr = " where " + filterStr
	} else {
		filterStr = ""
	}

	if offset == -1 || limit == -1 {
		sql = fmt.Sprintf("select * from tasks %s order by start_ts desc", filterStr)
	} else {
		sql = fmt.Sprintf("select * from tasks %s order by start_ts desc limit %d offset %d", filterStr, limit, offset)
	}
	_, err := sharedDbMap.Select(&tasks, sql)
	if err != nil {
		log.Warning(err)
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

func RemoveTasks(ts int64) {
	sharedDbMap.Exec("delete from tasks where update_ts<? and start_ts<?", ts, ts)
}
