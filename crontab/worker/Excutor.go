package worker

import (
	"Distributed-task-scheduling-system/crontab/common"
	"context"
	"os/exec"
	"time"

	"math/rand"
)

type Excutor struct {
}

var G_excutor *Excutor

func (execute *Excutor) ExectueJob(excuteInfo *common.JobExcuteInfo) (err error) {
	var (
		ctx        context.Context
		cancelFunc context.CancelFunc
		result     *common.JobFinishInfo
		jobLock    *JobLock
	)
	ctx, cancelFunc = context.WithCancel(context.TODO())
	excuteInfo.Ctx, excuteInfo.Cancel = ctx, cancelFunc
	jobLock = G_jobMgr.creatLock(excuteInfo.Job.JobName)
	time.Sleep(time.Duration(rand.Intn(G_config.RandomSleepTime)) * time.Millisecond)
	err = jobLock.Lock()
	defer jobLock.UnLock()
	if !jobLock.isLocked {
		result = &common.JobFinishInfo{
			JobExcuteInfo: excuteInfo,
			Output:        "抢锁失败，无法执行\n",
			Err:           err,
		}
		result.Err = err
		result.EndTime = time.Now()
		G_scheduler.PushResult(result)
		return
	} else {
		var (
			cmd       *exec.Cmd
			output    []byte
			startTime time.Time
			endTime   time.Time
		)
		startTime = time.Now()
		cmd = exec.CommandContext(ctx, "/bin/bash", "-c", excuteInfo.Job.Command) //修改contexts
		output, err = cmd.CombinedOutput()
		endTime = time.Now()
		result = &common.JobFinishInfo{
			JobExcuteInfo: excuteInfo,
			Output:        string(output),
			Err:           err,
			StartTime:     startTime,
			EndTime:       endTime,
		}
		excuteInfo.StartExcuteTime = startTime
		excuteInfo.EndExcuteRTime = endTime
		G_scheduler.PushResult(result)
		time.Sleep(time.Duration(rand.Intn(G_config.RandomSleepTime)+200) * time.Millisecond)
	}
	return
}

func InitExcutor() (err error) {
	G_excutor = &Excutor{}
	return
}
