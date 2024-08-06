package common

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/gorhill/cronexpr"
)

type TimePoint struct {
	StartTime string `json:"startTime"`
	EndTime   string `json:"endTime"`
}

type Job struct {
	JobName   string    `json:"jobName"`
	Command   string    `json:"command"`
	CronExpr  string    `json:"cronExpr"`
	Status    string    `json:"status"`
	Err       string    `json:"err"`
	TimePoint TimePoint `json:"timePoint"`
}

type Response struct {
	Errno int         `json:"errno"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

type JobSchedulePlan struct {
	Job      *Job
	CronExpr *cronexpr.Expression
	NextTime time.Time
}

type JobExcuteInfo struct {
	Job             *Job
	PlanStartTime   time.Time
	StartExcuteTime time.Time
	EndExcuteRTime  time.Time
	Ctx             context.Context
	Cancel          context.CancelFunc
}

type JobEvent struct {
	EventType int
	Job       *Job
}

type JobFinishInfo struct {
	JobExcuteInfo *JobExcuteInfo
	Output        string
	Err           error
	StartTime     time.Time
	EndTime       time.Time
}

type JobLog struct {
	IP              string    `bson:"IP"`
	JobName         string    `bson:"jobName"`
	Command         string    `bson:"command"`
	Err             string    `bson:"err"`
	OutPut          string    `bson:"output"`
	PlanStartTime   time.Time `bson:"planStartTime"`
	StartExcuteTime time.Time `bson:"startExcuteTime"`
	EndExcuteRTime  time.Time `bson:"endExcuteRTime"`
}

type LogBatch struct {
	Logs []interface{}
}

func BuildResponse(errno int, msg string, data interface{}) (resp []byte, err error) {
	response := &Response{
		Errno: errno,
		Msg:   msg,
		Data:  data,
	}
	resp, err = json.Marshal(response)
	return
}

func UnPack(bytes []byte) (job *Job, err error) {
	job = &Job{}
	if err = json.Unmarshal(bytes, job); err != nil {
		return
	}
	return
}

func ExtractIP(workerIp string) string {
	return strings.TrimPrefix(workerIp, JOB_WORKER)
}
func ExtractKiller(jobKey string) string {
	return strings.TrimPrefix(jobKey, JOB_KILLER_DIR)
}

func ExtractSave(jobKey string) string {
	return strings.TrimPrefix(jobKey, JOB_DIR)
}

func BuildJobEvent(job *Job, eventype int) (jobEvent *JobEvent) {
	jobEvent = &JobEvent{
		EventType: eventype,
		Job:       job,
	}
	return
}

func BuildJobSchedulePlan(job *Job) (jobSchedulePlan *JobSchedulePlan, err error) {
	var (
		expr     *cronexpr.Expression
		nextTime time.Time
	)
	if expr, err = cronexpr.Parse(job.CronExpr); err != nil {
		return
	}
	nextTime = expr.Next(time.Now())
	jobSchedulePlan = &JobSchedulePlan{
		Job:      job,
		CronExpr: expr,
		NextTime: nextTime,
	}
	return
}

// 这个是构建调度信息，通过从调度计划当中获取调度任务，加上时间构成调度执行信息
func BuildJobExcuteInfo(jobscheduleplan *JobSchedulePlan) (jobExcuteInfo *JobExcuteInfo) {
	jobExcuteInfo = &JobExcuteInfo{
		Job:           jobscheduleplan.Job,
		PlanStartTime: time.Now(),
	}
	return
}
