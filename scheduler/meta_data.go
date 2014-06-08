package scheduler

import (
	"fmt"
	"github.com/gorhill/cronexpr"
	log "github.com/ngaut/logging"
	"time"
)

// Job define
type Job struct {
	Id            int64  `db:"id" json:"id"`
	Name          string `db:"name" json:"name"`       // 512, unique
	Command       string `db:"command" json:"command"` // 4096
	Epsilon       string `db:"epsilon" json:"epsilon"`
	Executor      string `db:"executor" json:"executor"`             // 4096
	ExecutorFlags string `db:"executor_flags" json:"executor_flags"` // 4096
	Retries       int    `db:"retries" json:"retries"`
	Owner         string `db:"owner" json:"owner"`
	Async         bool   `db:"async" json:"async"`
	SuccessCnt    int    `db:"success_cnt" json:"success_cnt"`
	ErrCnt        int    `db:"error_cnt" json:"error_cnt"`
	CreateTs      int64  `db:"create_ts" json:"create_ts"`
	LastSuccessTs int64  `db:"last_success_ts" json:"last_success_ts"`
	LastErrTs     int64  `db:"last_error_ts" json:"last_error_ts"`
	Cpus          int    `db:"cpus" json:"cpus"`
	Mem           int    `db:"mem" json:"mem"`
	Disk          int64  `db:"disk" json:"disk"`
	Disabled      bool   `db:"disabled" json:"disabled"`
	Uris          string `db:"uris" json:"uris"` // 2048
}

// Dag meta define (job group)
type DagMeta struct {
	Id            int64  `db:"id" json:"id"`
	Name          string `db:"name" json:"name"`
	Schedule      string `db:"schedule" json:"schedule"`
	CreateTs      int64  `db:"create_ts" json:"create_ts"`
	LastSuccessTs int64  `db:"last_success_ts" json:"last_success_ts"`
	LastErrTs     int64  `db:"last_error_ts" json:"last_error_ts"`
	LastErrMsg    string `db:"last_error_msg" json:"last_error_msg"`
}

// Dag job (Edge)
type DagJob struct {
	Id        int64  `db:"id" json:"id"`
	DagName   string `db:"dag_name" json:"dag_name"`
	JobName   string `db:"job_name" json:"job_name"`
	ParentJob string `db:"parent" json:"parent"`
}

func GetJobList() []Job {
	var jobs []Job
	_, err := sharedDbMap.Select(&jobs, "select * from jobs order by create_ts desc")
	if err != nil {
		return nil
	}
	return jobs
}

func JobExists(name string) bool {
	j, err := GetJobByName(name)
	if err != nil {
		log.Error(err, name)
		return false
	}

	if j.Id == 0 {
		return false
	}

	return true
}

func GetJobByName(name string) (*Job, error) {
	var job Job
	err := sharedDbMap.SelectOne(&job, "select * from jobs where name=?", name)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func GetJobById(id int) (*Job, error) {
	var job Job
	err := sharedDbMap.SelectOne(&job, "select * from jobs where id=?", id)
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

// DAG
func NewDagMeta(name, schedule string) *DagMeta {
	return &DagMeta{
		Name:     name,
		Schedule: schedule,
		CreateTs: time.Now().Unix(),
	}
}

func NewDagJob(jobName, parentJobName string) *DagJob {
	return &DagJob{
		JobName:   jobName,
		ParentJob: parentJobName,
	}
}

func GetDagMetaList() []DagMeta {
	var dags []DagMeta
	_, err := sharedDbMap.Select(&dags, "select * from dagmeta order by create_ts desc")
	if err != nil {
		return nil
	}
	return dags
}

func GetDagFromName(name string) *DagMeta {
	var dag DagMeta
	err := sharedDbMap.SelectOne(&dag, "select * from dagmeta where name=?", name)
	if err != nil {
		return nil
	}
	return &dag
}

func (j *DagJob) Save() error {
	if j.Id <= 0 {
		return sharedDbMap.Insert(j)
	} else {
		_, err := sharedDbMap.Update(j)
		return err
	}
}

func (j *DagJob) Remove() {
	sharedDbMap.Delete(j)
}

func (j DagJob) String() string {
	return fmt.Sprintf("DagJob{DagName:%s, Id:%d, JobName:%s, ParentJob:%s}",
		j.DagName, j.Id, j.JobName, j.ParentJob)
}

func (dag *DagMeta) GetDagJobs() []DagJob {
	var dagJobs []DagJob
	_, err := sharedDbMap.Select(&dagJobs, "select * from dagjobs where dag_name = ?", dag.Name)
	if err != nil {
		log.Error(err)
		return nil
	}
	return dagJobs
}

func (dag *DagMeta) AutoRunSignal() (bool, <-chan *DagMeta) {
	c := make(chan *DagMeta)
	if len(dag.Schedule) <= 0 {
		return false, nil
	}

	go func() {
		for {
			now := time.Now()
			nextTime := cronexpr.MustParse(dag.Schedule).Next(now)
			dur := nextTime.Sub(now)
			select {
			case <-time.After(dur):
				{
					c <- dag
				}
			}
		}
	}()
	return true, c
}

func (d *DagMeta) AddDagJob(j *DagJob) error {
	j.DagName = d.Name

	var dagJobs []DagJob
	_, err := sharedDbMap.Select(&dagJobs, "select * from dagjobs where dag_name = ? and job_name == ?", d.Name, j.JobName)
	if err != nil {
		return err
	}

	if len(dagJobs) > 0 {
		return fmt.Errorf("job %s already exist", j.JobName)
	}

	return sharedDbMap.Insert(j)
}

func (d *DagMeta) Save() error {
	if d.Id <= 0 {
		return sharedDbMap.Insert(d)
	} else {
		_, err := sharedDbMap.Update(d)
		return err
	}
}

func (d *DagMeta) Remove() error {
	if d.Id > 0 {
		for _, j := range d.GetDagJobs() {
			(&j).Remove()
		}
		cnt, err := sharedDbMap.Delete(d)
		if cnt == 1 && err == nil {
			d.Id = -1
			return nil
		}
		return err
	}
	d.Id = -1
	return nil
}
