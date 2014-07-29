package scheduler

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-martini/martini"
	"github.com/martini-contrib/render"
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

type User string

var (
	s          *Server
	ldapEnable bool
)

type PageInfo struct {
	page   int
	limit  int
	offset int
}

type FilterInfo map[string]string

func (f FilterInfo) Statement() string {
	var filterFields []string
	// create where statement
	for k, v := range f {
		filterFields = append(filterFields, k+`="`+v+`"`)
	}
	var filterStr string
	if len(filterFields) > 0 {
		filterStr = strings.Join(filterFields, " and ")
		filterStr = " where " + filterStr
	} else {
		filterStr = ""
	}
	return filterStr
}

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

func jobUpdate(params martini.Params, req *http.Request, user User) (int, string) {
	id := params["id"]
	if JobExists(id) {
		b, err := ioutil.ReadAll(req.Body)
		if err != nil {
			return responseError(-2, err.Error())
		}
		var job Job
		err = json.Unmarshal(b, &job)
		j, _ := GetJobById(id)

		if ldapEnable && j != nil && j.Owner != string(user) {
			return responseError(-3, "user not allowed")
		}

		if err != nil {
			return responseError(-3, err.Error())
		}

		j.Name = job.Name
		j.Executor = job.Executor
		j.ExecutorFlags = job.ExecutorFlags
		j.Uris = job.Uris
		j.Schedule = job.Schedule
		j.WebHookUrl = job.WebHookUrl

		if err := j.Save(); err != nil {
			return responseError(-4, err.Error())
		}
		return responseSuccess(j)
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

func jobRemove(params martini.Params, user User) (int, string) {
	id := params["id"]
	log.Debug("on job remove")
	j, _ := GetJobById(id)

	if ldapEnable && j != nil && j.Owner != string(user) {
		return responseError(-3, "user not allowed")
	}

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

func jobRun(params martini.Params, user User) (int, string) {
	id := params["id"]
	j, err := GetJobById(id)
	if err != nil {
		return responseError(-1, err.Error())
	}

	if ldapEnable && j != nil && j.Owner != string(user) {
		return responseError(-3, "user not allowed")
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

func taskKill(params martini.Params, user User) string {
	id := params["id"]
	task, err := GetTaskByTaskId(id)
	if err != nil {
		log.Debug(err)
		return err.Error()
	}

	if task != nil {
		j, err := GetJobByName(task.JobName)
		if err != nil {
			log.Debug(err)
			return err.Error()
		}
		if ldapEnable && j != nil && j.Owner != string(user) {
			return "user not allowed"
		}
	}

	if s.notifier != nil {
		err := s.notifier.OnKillTask(id)
		if err != nil {
			return err.Error()
		}
		return "OK"
	}

	return "error:notifier not registered"
}

func lookup_ldap(username, password string) bool {
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
	return true
}

func authenticate(res http.ResponseWriter, req *http.Request, c martini.Context) {
	auth := req.Header.Get("Authorization")
	if len(auth) < 6 || auth[:6] != "Basic " {
		unauthorized(res)
		return
	}
	b, err := base64.StdEncoding.DecodeString(auth[6:])
	if err != nil {
		unauthorized(res)
		return
	}
	tokens := strings.SplitN(string(b), ":", 2)
	if len(tokens) != 2 || !lookup_ldap(tokens[0], tokens[1]) {
		unauthorized(res)
		return
	}
	c.Map(User(tokens[0]))
	cookie := http.Cookie{Name: "username", Value: tokens[0], Path: "/"}
	http.SetCookie(res, &cookie)
}

func filter(req *http.Request, c martini.Context) {
	req.ParseForm()
	m := make(FilterInfo)
	for k, v := range req.Form {
		if strings.HasPrefix(k, "f_") {
			field := strings.Split(k, "f_")[1]
			m[field] = v[0]
		}
	}
	c.Map(m)
}

func pagination(req *http.Request, c martini.Context) {
	req.ParseForm()
	page, err := strconv.Atoi(req.FormValue("page"))
	if err != nil {
		page = 1
	}
	limit, err := strconv.Atoi(req.FormValue("limit"))
	if err != nil {
		limit = 20
	}
	c.Map(PageInfo{
		page:   page,
		limit:  limit,
		offset: (page - 1) * limit,
	})
}

func unauthorized(res http.ResponseWriter) {
	res.Header().Set("WWW-Authenticate", "Basic realm=\"Authorization Required\"")
	http.Error(res, "Not Authorized", http.StatusUnauthorized)
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

func slice(n int64) []int { return make([]int, n) }

// V2 APIs
func jobPageV2(req *http.Request, user User, r render.Render, p PageInfo) {
	jobs := GetJobListWithOffset(p.offset, p.limit)
	jobsCnt, err := GetTotalJobCount()
	if err != nil {
		log.Warning(err)
		jobsCnt = 0
	}

	r.HTML(200, "job", map[string]interface{}{
		"jobs":     jobs,
		"max_page": slice(jobsCnt/int64(p.limit) + 1),
		"limit":    p.limit,
		"cur_page": p.page,
	})
}

func taskPageV2(req *http.Request, user User, params martini.Params, r render.Render, p PageInfo, filter FilterInfo) {
	tasks := GetTaskListWithOffsetAndFilter(p.offset, p.limit, filter)
	taskCnt, err := GetTotalTaskCount(filter)
	if err != nil {
		log.Warning(err)
		taskCnt = 0
	}

	r.HTML(200, "status", map[string]interface{}{
		"tasks":    tasks,
		"max_page": slice(taskCnt/int64(p.limit) + 1),
		"limit":    p.limit,
		"cur_page": p.page,
	})
}

func (srv *Server) Serve() {
	m := martini.Classic()

	addr, _ := globalCfg.ReadString("http_addr", "9090")
	ldapOption, _ := globalCfg.ReadString("ldapEnable", "false")
	ldapEnable = ldapOption == "true"

	m.Use(martini.Static("static"))
	m.Map(User(""))

	if ldapEnable {
		m.Use(authenticate)
	} else {
		m.Use(func(res http.ResponseWriter) {
			cookie := http.Cookie{Name: "username", Value: "", Path: "/"}
			http.SetCookie(res, &cookie)
		})
	}

	m.Use(pagination)
	m.Use(filter)

	m.Use(render.Renderer(render.Options{
		Directory:  "templates",
		Extensions: []string{".tmpl", ".html"},
		Charset:    "UTF-8",
		Funcs: []template.FuncMap{
			template.FuncMap{
				"add": func(a, b int) int {
					return a + b
				},
				"ts_to_date": func(ts int64) string {
					if ts > 0 {
						t := time.Unix(ts, 0)
						return t.Format("2006-01-02 15:04:05")
					}
					return "NEVER"
				},
			},
		},
		IndentJSON: true, // Output human readable JSON
	}))

	m.Use(martini.Static("static"))

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

	// V2 APIs
	m.Get("/v2/job", jobPageV2)
	m.Get("/v2/status", taskPageV2)
	m.Get("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/v2/job", http.StatusFound)
	})

	os.Setenv("PORT", addr)
	m.Run()
}
