package scheduler

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"time"

	"github.com/go-martini/martini"
	"github.com/martini-contrib/auth"
	"github.com/mqu/openldap"
	log "github.com/ngaut/logging"
)

type Notifier interface {
	OnRunJob(j *Job) (string, error) //taskId, error
	GetStatusByTaskId(taskId string) (string, error)
	OnKillTask(taskId string) error
}

type Server struct {
	addr     string
	notifier Notifier
}

var (
	s *Server
)

func NewServer(addr string, notifier Notifier) *Server {
	if s != nil {
		return s
	}
	s = &Server{addr, notifier}
	return s
}

func responseJson(statusCode int, obj interface{}) (int, string) {
	if obj != nil {
		content, _ := json.MarshalIndent(obj, " ", "  ")
		return statusCode, string(content)
	}
	return statusCode, ""
}

func responseError(ret int, msg string) (int, string) {
	return responseJson(500, map[string]interface{}{
		"ret": ret,
		"msg": msg,
	})
}

func responseSuccess(data interface{}) (int, string) {
	return responseJson(200, map[string]interface{}{
		"ret":  0,
		"data": data,
	})
}

func jobList() (int, string) {
	jobs := GetJobList()
	if jobs != nil && len(jobs) > 0 {
		return responseSuccess(jobs)
	}

	return responseSuccess(nil)
}

func jobUpdate(params martini.Params, req *http.Request) (int, string) {
	id := params["id"]
	if JobExists(id) {
		b, err := ioutil.ReadAll(req.Body)
		if err != nil {
			return responseError(-2, err.Error())
		}
		var job Job
		err = json.Unmarshal(b, &job)
		j, _ := GetJobById(id)
		if err != nil {
			return responseError(-3, err.Error())
		}
		job.Id = j.Id
		if err := job.Save(); err != nil {
			return responseError(-4, err.Error())
		}
		return responseSuccess(job)
	}

	return responseError(-5, "no such job")
}

func jobNew(req *http.Request) (int, string) {
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return responseError(-1, err.Error())
	}

	var job Job
	err = json.Unmarshal(b, &job)
	if err != nil {
		return responseError(-2, err.Error())
	}

	job.CreateTs = time.Now().Unix()
	err = sharedDbMap.Insert(&job)
	if err != nil {
		return responseError(-3, err.Error())
	}

	return responseSuccess(job)
}

func jobRemove(params martini.Params) (int, string) {
	id := params["id"]
	log.Debug("on job remove")
	j, _ := GetJobById(id)
	if j != nil {
		if err := j.Remove(); err != nil {
			return responseError(-2, err.Error())
		}
		return responseSuccess(j)
	}

	return responseError(-3, "no such job")
}

func jobGet(params martini.Params) (int, string) {
	id := params["id"]
	j, err := GetJobById(id)
	if err != nil {
		return responseError(-1, err.Error())
	}

	return responseSuccess(j)
}

func jobRun(params martini.Params) (int, string) {
	id := params["id"]
	j, err := GetJobById(id)
	if err != nil {
		return responseError(-1, err.Error())
	}

	if s.notifier != nil && j != nil {
		taskId, err := s.notifier.OnRunJob(j)
		if err != nil {
			log.Debug(err.Error())
			return responseError(-2, err.Error())
		}

		return responseSuccess(taskId)
	}

	log.Debug("Notifier not found")
	return responseError(-3, "notifier not found")
}

func jobRunOnce(req *http.Request) (int, string) {
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return responseError(-1, err.Error())
	}
	var j Job
	err = json.Unmarshal(b, &j)
	if err != nil {
		return responseError(-2, err.Error())
	}

	jobTpl, err := GetJobById(strconv.Itoa(int(j.Id)))
	if err != nil {
		return responseError(-1, err.Error())
	}

	j.Owner = jobTpl.Owner
	j.Disk = jobTpl.Disk
	j.Mem = jobTpl.Mem
	j.Cpus = jobTpl.Cpus
	j.Name = jobTpl.Name

	j.CreateTs = time.Now().Unix()
	if s.notifier != nil {
		taskId, err := s.notifier.OnRunJob(&j)
		if err != nil {
			log.Debug(err.Error())
			return responseError(-2, err.Error())
		}

		return responseSuccess(taskId)
	}

	log.Debug("Notifier not found")
	return responseError(-3, "notifier not found")
}

func taskList() (int, string) {
	tasks := GetTaskList()
	if tasks != nil && len(tasks) > 0 {
		return responseSuccess(tasks)
	}

	return responseSuccess(nil)
}

func taskKill(params martini.Params) string {
	id := params["id"]
	if s.notifier != nil {
		err := s.notifier.OnKillTask(id)
		if err != nil {
			return err.Error()
		}
		return "OK"
	}

	return "error:notifier not registered"
}

func authenticate(username, password string) bool {
	ldap_server, _ := globalCfg.ReadString("ldap_server", "")
	dn_fmt, _ := globalCfg.ReadString("dn_fmt", "")
	ldap, err := openldap.Initialize(ldap_server)
	if err != nil {
		log.Error(err)
		return false
	}

	ldap.SetOption(openldap.LDAP_OPT_PROTOCOL_VERSION, openldap.LDAP_VERSION3)
	dn := fmt.Sprintf(dn_fmt, username)
	err = ldap.Bind(dn, password)
	if err != nil {
		log.Warning(err)
		return false
	}

	ldap.Close()
	return err == nil
}

func gc() {
	tick := time.NewTicker(30 * time.Minute)
	for {
		select {
		case <-tick.C:
			log.Debug(time.Now().Unix())
			//3 days before, todo: read it from config
			RemoveTasks(time.Now().Unix() - 3*(24*3600))
		}
	}
}

func (srv *Server) Serve() {
	m := martini.Classic()

	addr, _ := globalCfg.ReadString("http_addr", "9090")
	ldap_enable, _ := globalCfg.ReadString("ldap_enable", "false")

	m.Use(martini.Static("static"))

	if ldap_enable == "true" {
		m.Use(auth.BasicFunc(authenticate))
	}

	go gc()

	m.Get("/job/list", jobList)
	m.Get("/task/list", taskList)
	m.Get("/task/kill/:id", taskKill)
	m.Get("/job/:id", jobGet)
	m.Post("/job", jobNew)
	m.Post("/job/runonce", jobRunOnce)
	m.Post("/job/run/:id", jobRun)
	m.Delete("/job/:id", jobRemove)
	m.Put("/job/:id", jobUpdate)

	os.Setenv("PORT", addr)
	m.Run()
}
