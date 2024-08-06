package master

import (
	common "Distributed-task-scheduling-system/crontab/common"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

var (
	G_apiServer *ApiServer
)

type ApiServer struct {
	httpServer *http.Server
}

// {···,job:{jobname:xxx,command:xxx,cronexpr:xxx,status:xxx,err:xxx,timepoint:{starttime:xxx,entime:xxx}}，···}
func handleSaveJob(res http.ResponseWriter, req *http.Request) {
	var (
		err     error
		postJob string
		job     *common.Job
		oldJob  *common.Job
		bytes   []byte
	)
	if err = req.ParseForm(); err != nil {
		goto ERR
	}
	postJob = req.PostForm.Get("job")
	job = &common.Job{}
	if err = json.Unmarshal([]byte(postJob), job); err != nil {
		goto ERR
	}
	if oldJob, err = G_jobMgr.SaveJob(job); err != nil {
		goto ERR
	}
	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		res.Write(bytes)
	}
	log.WithFields(log.Fields{
		"crateJob": job.JobName,
		"command":  job.Command,
		"cronexpr": job.CronExpr,
	}).Info("request to create job successfully")
	return
ERR:
	log.WithFields(log.Fields{"err": err.Error()}).Info("request to create job Failed")
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		res.Write(bytes)
	}
}
func handleDeleteJob(res http.ResponseWriter, req *http.Request) {
	var (
		bytes   []byte
		err     error
		postJob string
		delJob  *common.Job
		oldJob  *common.Job
	)
	if err = req.ParseForm(); err != nil {
		goto ERR
	}
	postJob = req.PostForm.Get("job")
	delJob = &common.Job{}
	if err = json.Unmarshal([]byte(postJob), delJob); err != nil {
		goto ERR
	}
	if oldJob, err = G_jobMgr.DeleteJob(delJob); err != nil {
		goto ERR
	}
	if bytes, err = common.BuildResponse(0, "success", oldJob); err != nil {
		goto ERR
	}
	res.Write(bytes)
	log.WithFields(log.Fields{
		"delJob": delJob.JobName,
		"oldjob": oldJob.JobName,
	}).Info("request to delete job successfully")
	return
ERR:
	log.WithFields(log.Fields{"err": err.Error()}).Info("request to delete job Failed")
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		res.Write(bytes)
	}
}
func handleListAll(res http.ResponseWriter, req *http.Request) {
	var (
		allJob []*common.Job
		bytes  []byte
		err    error
	)
	if allJob, err = G_jobMgr.ListAllJob(); err != nil {
		goto ERR
	}
	if bytes, err = common.BuildResponse(0, "success", allJob); err != nil {
		goto ERR
	}
	res.Write(bytes)
	log.Info("Get all jobs successfully!")
	return
ERR:
	log.WithFields(log.Fields{"err": err.Error()}).Warn("Get all jobs failed")
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err != nil {
		res.Write(bytes)
	}
}

// 向etcd的killer当中存放json信息，woker节点可以通过watcher.key.value来获取放入的信息，解析得到要杀死的任务名称
func handleKillJob(res http.ResponseWriter, req *http.Request) {
	var (
		err     error
		bytes   []byte
		jobName string
	)
	if err = req.ParseForm(); err != nil {
		goto ERR
	}
	jobName = req.PostForm.Get("job")
	if err = G_jobMgr.KillJob(jobName); err != nil {
		goto ERR
	}
	if bytes, err = common.BuildResponse(0, "success", nil); err != nil {
		goto ERR
	}
	if _, err = res.Write(bytes); err != nil {
		goto ERR
	}
	log.WithFields(log.Fields{"jobName": jobName}).Info("kill job successfully!")
	return
ERR:
	log.WithFields(log.Fields{
		"err":     err.Error(),
		"jobName": jobName,
	}).Warn("Kill job failed")
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		res.Write(bytes)
	}
}

func handleHealthWorker(res http.ResponseWriter, req *http.Request) {
	var (
		healthWorkers []string
		bytes         []byte
		err           error
	)
	if healthWorkers, err = G_jobMgr.GetHealthWorker(); err != nil {
		goto ERR
	}
	if bytes, err = common.BuildResponse(0, "success", healthWorkers); err != nil {
		goto ERR
	}
	res.Write(bytes)
	return
ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err != nil {
		res.Write(bytes)
	}
}

func InitApiServer() (err error) {
	var (
		mux        *http.ServeMux
		listener   net.Listener
		httpServer *http.Server
	)
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleSaveJob)
	mux.HandleFunc("/job/delete", handleDeleteJob)
	mux.HandleFunc("/job/listall", handleListAll)
	mux.HandleFunc("/job/killjob", handleKillJob)
	mux.HandleFunc("/health", handleHealthWorker)
	if listener, err = net.Listen("tcp", ":"+strconv.Itoa(G_config.ListenPort)); err != nil {
		return
	}
	httpServer = &http.Server{
		ReadTimeout:  time.Duration(G_config.ReadTimepOut) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.WriteTimeOut) * time.Millisecond,
		Handler:      mux,
	}
	G_apiServer = &ApiServer{
		httpServer: httpServer,
	}
	go func() {
		httpServer.Serve(listener)
	}()
	return
}
