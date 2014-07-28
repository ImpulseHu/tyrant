package scheduler

import (
	"fmt"
	"strconv"
	"time"

	"github.com/gorhill/cronexpr"
	log "github.com/ngaut/logging"
)

// Job define
type Job struct {
	Id            int64  `db:"id" json:"id"`
	Name          string `db:"name" json:"name"`                     // 512, unique
	Executor      string `db:"executor" json:"executor"`             // 4096
	ExecutorFlags string `db:"executor_flags" json:"executor_flags"` // 4096
	Retries       int    `db:"retries" json:"retries"`
	Owner         string `db:"owner" json:"owner"`
	SuccessCnt    int    `db:"success_cnt" json:"success_cnt"`
	ErrCnt        int    `db:"error_cnt" json:"error_cnt"`
	CreateTs      int64  `db:"create_ts" json:"create_ts"`
	LastTaskId    string `db:"last_task_id" json:"last_task_id"`
	LastSuccessTs int64  `db:"last_success_ts" json:"last_success_ts"`
	LastErrTs     int64  `db:"last_error_ts" json:"last_error_ts"`
	LastStatus    string `db:"last_status" json:"last_status"`
	Cpus          int    `db:"cpus" json:"cpus"`
	Mem           int    `db:"mem" json:"mem"`
	Disk          int64  `db:"disk" json:"disk"`
	Disabled      bool   `db:"disabled" json:"disabled"`
	Uris          string `db:"uris" json:"uris"` // 2048, using comma to split
	Schedule      string `db:"schedule" json:"schedule"`
	WebHookUrl    string `db:"hook" json:"hook"`
}

func GetTotalJobCount() (int64, error) {
	cnt, err := sharedDbMap.SelectInt("select count(*) from jobs")
	if err != nil {
		return -1, err
	}
	return cnt, nil
}

func GetJobList() []Job {
	return GetJobListWithOffset(-1, -1)
}

func GetJobListWithOffset(offset int, limit int) []Job {
	var jobs []Job
	sql := ""
	if offset == -1 || limit == -1 {
		sql = "select * from jobs order by create_ts"
	} else {
		sql = fmt.Sprintf("select * from jobs order by create_ts limit %d offset %d", limit, offset)
	}
	_, err := sharedDbMap.Select(&jobs, sql)
	if err != nil {
		log.Warning(err)
		return nil
	}
	return jobs
}

func JobExists(id string) bool {
	j, err := GetJobById(id)
	if err != nil {
		log.Error(err, id)
		return false
	}

	if j.Id == 0 {
		return false
	}
	return true
}

func GetScheduledJobList() []Job {
	var jobs []Job
	_, err := sharedDbMap.Select(&jobs, "select * from jobs where schedule <> ''")
	if err != nil {
		log.Warning(err.Error())
		return nil
	}
	return jobs
}

func CheckAutoRunJobs(n Notifier) {
	for {
		log.Debug("start check auto run job...")
		jobs := GetScheduledJobList()
		for i, j := range jobs {
			if j.NeedAutoStart() {
				log.Debug("Auto Run Job Found: ", j)
				n.OnRunJob(&jobs[i])
			}
		}
		time.Sleep(10 * time.Second)
	}
}

func GetJobById(id string) (*Job, error) {
	nid, err := strconv.Atoi(id)
	if err != nil {
		return nil, err
	}
	var job Job
	err = sharedDbMap.SelectOne(&job, "select * from jobs where id=?", nid)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func GetJobByName(name string) (*Job, error) {
	var job Job
	err := sharedDbMap.SelectOne(&job, "select * from jobs where name=?", name)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (j *Job) Disable(b bool) error {
	j.Disabled = b
	_, err := sharedDbMap.Update(j)
	return err
}

func (j *Job) GetLastRunTime() int64 {
	if len(j.LastTaskId) == 0 {
		return 0
	}
	task, _ := GetTaskByTaskId(j.LastTaskId)
	if task != nil {
		return task.StartTs
	}
	return 0
}

// goroutine run this function periodly to check if this job is needed to auto start
func (j Job) NeedAutoStart() bool {
	if len(j.Schedule) > 0 {
		expr, err := cronexpr.Parse(j.Schedule)
		if err != nil {
			log.Debug(err.Error())
			return false
		}
		last_run_ts := j.GetLastRunTime()
		if last_run_ts >= 0 {
			last_run_time := time.Unix(last_run_ts, 0)
			next_time := expr.Next(last_run_time)
			log.Debug("next_time", next_time)
			// > 20s
			if time.Now().Unix()-next_time.Unix() > 20 {
				return true
			}
		}
	}
	return false
}

func (j *Job) Save() error {
	if j.Id <= 0 {
		return sharedDbMap.Insert(j)
	} else {
		_, err := sharedDbMap.Update(j)
		return err
	}
}

func (j *Job) Remove() error {
	if j.Id > 0 {
		cnt, err := sharedDbMap.Delete(j)
		if cnt == 1 && err == nil {
			j.Id = -1
			return nil
		}
		return err
	}
	j.Id = -1
	return nil
}
