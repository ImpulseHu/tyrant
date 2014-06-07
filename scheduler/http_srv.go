package scheduler

import (
	"encoding/json"
	"io/ioutil"
	_ "net/http/pprof"

	"github.com/hoisie/web"
)

var indexPageContent string

func init() {
	b, _ := ioutil.ReadFile("./templates/index.html")
	indexPageContent = string(b)
}

type Notifier interface {
	OnRunJob(name string) (string, error)
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
	return responseSuccess(ctx, "[]")
}

func jobUpdate(ctx *web.Context) string {
	name, b := ctx.Params["name"]
	if !b {
		return responseError(ctx, -1, "job name is needed")
	}

	if JobExists(name) {
		b, err := ioutil.ReadAll(ctx.Request.Body)
		if err != nil {
			return responseError(ctx, -2, err.Error())
		}
		var job Job
		err = json.Unmarshal(b, &job)
		j, _ := GetJobByName(name)
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
	if err != nil {
		return responseError(ctx, -2, err.Error())
	}
	err = sharedDbMap.Insert(&job)
	if err != nil {
		return responseError(ctx, -3, err.Error())
	}
	return responseSuccess(ctx, job)
}

func jobRemove(ctx *web.Context) string {
	name, b := ctx.Params["name"]
	if !b {
		return responseError(ctx, -1, "job name is needed")
	}
	j, _ := GetJobByName(name)
	if j != nil {
		if err := j.Remove(); err != nil {
			return responseError(ctx, -2, err.Error())
		}
		return responseSuccess(ctx, j)
	}
	return responseError(ctx, -3, "no such job")
}

func dagList(ctx *web.Context) string {
	return ""
}

func dagNew(ctx *web.Context) string {
	b, err := ioutil.ReadAll(ctx.Request.Body)
	if err != nil {
		return responseError(ctx, -1, err.Error())
	}
	var dag DagMeta
	err = json.Unmarshal(b, &dag)
	if err != nil {
		return responseError(ctx, -2, err.Error())
	}
	err = dag.Save()
	if err != nil {
		return responseError(ctx, -3, err.Error())
	}
	return responseSuccess(ctx, dag)
}

func dagJobAdd(ctx *web.Context) string {
	name, ok := ctx.Params["name"]
	if !ok {
		return responseError(ctx, -1, "dag job name is needed")
	}
	b, err := ioutil.ReadAll(ctx.Request.Body)
	if err != nil {
		return responseError(ctx, -2, err.Error())
	}
	dag := GetDagFromName(name)
	if dag != nil {
		var job DagJob
		err = json.Unmarshal(b, &job)
		if err != nil {
			return responseError(ctx, -3, err.Error())
		}
		err = dag.AddDagJob(&job)
		if err != nil {
			return responseError(ctx, -4, err.Error())
		}
		return responseSuccess(ctx, job)
	}
	return responseError(ctx, -5, "same job exists")
}

func dagJobRemove(ctx *web.Context) string {
	name, ok := ctx.Params["name"]
	if !ok {
		return responseError(ctx, -1, "dag job name is needed")
	}
	dag := GetDagFromName(name)
	if dag != nil {
		err := dag.Remove()
		if err != nil {
			return responseError(ctx, -2, err.Error())
		}
		return responseSuccess(ctx, "")
	} else {
		return responseError(ctx, -3, "no such dag job")
	}
}

func dagJobRun(ctx *web.Context) string {
	name, ok := ctx.Params["name"]
	if !ok {
		return responseError(ctx, -1, "dag job name is needed")
	}

	if s.notifier != nil {
		taskId, err := s.notifier.OnRunJob(name)
		if err != nil {
			return responseError(ctx, -2, err.Error())
		}
		return responseSuccess(ctx, taskId)
	}

	return responseError(ctx, -3, "notifier not found")
}

func indexPage(ctx *web.Context) string {
	return indexPageContent
}

func (srv *Server) Serve() {
	web.Get("/", indexPage)
	web.Get("/job/list", jobList)
	web.Post("/job/new", jobNew)
	web.Post("/job/remove", jobRemove)
	web.Post("/job/update", jobUpdate)
	web.Post("/dag/new", dagNew)
	web.Post("/dag/job/add", dagJobAdd)
	web.Post("/dag/job/remove", dagJobRemove)
	web.Post("/dag/job/run", dagJobRun)
	addr, _ := globalCfg.ReadString("http_addr", ":9090")
	web.Run(addr)
}
