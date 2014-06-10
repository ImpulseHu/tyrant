package scheduler

import (
	"encoding/json"
	"io/ioutil"
	_ "net/http/pprof"
	"time"

	"github.com/hoisie/web"
	log "github.com/ngaut/logging"
)

type Notifier interface {
	OnRunJob(name string) (string, error) //taskId, error
	GetStatusByTaskId(taskId string) (string, error)
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

func responseJson(ctx *web.Context, statusCode int, obj interface{}) string {
	ctx.WriteHeader(statusCode)
	if obj != nil {
		content, _ := json.MarshalIndent(obj, " ", "  ")
		return string(content)
	}
	return ""
}

func responseError(ctx *web.Context, ret int, msg string) string {
	return responseJson(ctx, 500, map[string]interface{}{
		"ret": ret,
		"msg": msg,
	})
}

func responseSuccess(ctx *web.Context, data interface{}) string {
	return responseJson(ctx, 200, map[string]interface{}{
		"ret":  0,
		"data": data,
	})
}

func jobList(ctx *web.Context) string {
	jobs := GetJobList()
	if jobs != nil && len(jobs) > 0 {
		return responseSuccess(ctx, jobs)
	}
	return responseSuccess(ctx, nil)
}

func jobUpdate(ctx *web.Context, id string) string {
	if JobExists(id) {
		b, err := ioutil.ReadAll(ctx.Request.Body)
		if err != nil {
			return responseError(ctx, -2, err.Error())
		}
		var job Job
		err = json.Unmarshal(b, &job)
		j, _ := GetJobById(id)
		if err != nil {
			return responseError(ctx, -3, err.Error())
		}
		job.Id = j.Id
		if err := job.Save(); err != nil {
			return responseError(ctx, -4, err.Error())
		}
		return responseSuccess(ctx, job)
	} else {
		return responseError(ctx, -5, "no such job")
	}
}

func jobNew(ctx *web.Context) string {
	b, err := ioutil.ReadAll(ctx.Request.Body)
	if err != nil {
		return responseError(ctx, -1, err.Error())
	}
	var job Job
	err = json.Unmarshal(b, &job)
	job.CreateTs = time.Now().Unix()
	if err != nil {
		return responseError(ctx, -2, err.Error())
	}
	err = sharedDbMap.Insert(&job)
	if err != nil {
		return responseError(ctx, -3, err.Error())
	}
	return responseSuccess(ctx, job)
}

func jobRemove(ctx *web.Context, id string) string {
	log.Debug("on job remove")
	j, _ := GetJobById(id)
	if j != nil {
		if err := j.Remove(); err != nil {
			return responseError(ctx, -2, err.Error())
		}
		return responseSuccess(ctx, j)
	}
	return responseError(ctx, -3, "no such job")
}

func jobGet(ctx *web.Context, id string) string {
	j, err := GetJobById(id)
	if err != nil {
		return responseError(ctx, -1, err.Error())
	}
	return responseSuccess(ctx, j)
}

func jobRun(ctx *web.Context, id string) string {
	j, err := GetJobById(id)
	if err != nil {
		return responseError(ctx, -1, err.Error())
	}

	if s.notifier != nil && j != nil {
		taskId, err := s.notifier.OnRunJob(id)
		if err != nil {
			log.Debug(err.Error())
			return responseError(ctx, -2, err.Error())
		}
		return responseSuccess(ctx, taskId)
	}
	log.Debug("Notifier not found")
	return responseError(ctx, -3, "notifier not found")
}

func (srv *Server) Serve() {
	web.Get("/job/list", jobList)
	web.Get("/job/(.*)", jobGet)
	web.Post("/job", jobNew)
	web.Post("/job/run/(.*)", jobRun)
	web.Delete("/job/(.*)", jobRemove)
	web.Put("/job/(.*)", jobUpdate)

	addr, _ := globalCfg.ReadString("http_addr", ":9090")
	web.Run(addr)
}
