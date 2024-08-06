package worker

import (
	"Distributed-task-scheduling-system/crontab/common"
	"fmt"
	"sync"
	"time"
)

type Scheduler struct {
	JobEventChan     chan *common.JobEvent
	JobFinishInfo    chan *common.JobFinishInfo
	NearTimer        *time.Timer
	JobScheduleTable sync.Map /*map[string]*common.JobSchedulePlan*/
	JobExcutingTable sync.Map /*map[string]*common.Job*/
}

var G_scheduler *Scheduler

func (scheduler *Scheduler) TryStartJob(jobExcuteInfo *common.JobExcuteInfo) (err error) {
	var (
		excuting bool
	)
	if _, excuting = scheduler.JobExcutingTable.Load(jobExcuteInfo.Job.JobName); excuting {
		fmt.Printf("%v is excuting!\n", jobExcuteInfo.Job.JobName)
		return
	}
	scheduler.JobExcutingTable.Store(jobExcuteInfo.Job.JobName, jobExcuteInfo)
	jobExcuteInfo.StartExcuteTime = time.Now()
	G_excutor.ExectueJob(jobExcuteInfo)
	return
}

/*
* 遍历调度表，把当前时间和调度表里面每一个任务调度时间时间
* 如果过期就进行tryshedule进行尝试调度
* 并且返回给调度器下次的刷新时间到最近的一次时间
* 如果调度表没有元素，则休眠一秒钟
 */
func (scheduler *Scheduler) TrySchedule() (nextSchedule time.Duration) {
	var (
		jobSchedulePlan *common.JobSchedulePlan
		now             time.Time
		nearTime        *time.Time
		jobExcuteInfo   *common.JobExcuteInfo
	)
	now = time.Now()
	scheduler.JobScheduleTable.Range(func(key, value interface{}) bool {
		jobSchedulePlan = value.(*common.JobSchedulePlan)
		// 是否调度
		if jobSchedulePlan.NextTime.Before(now) || jobSchedulePlan.NextTime.Equal(now) {
			// 从计划调度信息转变为执行信息
			jobExcuteInfo = common.BuildJobExcuteInfo(jobSchedulePlan)
			go scheduler.TryStartJob(jobExcuteInfo)
		}
		// 计算后，比较最近的下一次执行时间
		jobSchedulePlan.NextTime = jobSchedulePlan.CronExpr.Next(time.Now())
		if nearTime == nil || jobSchedulePlan.NextTime.Before(*nearTime) {
			nearTime = &jobSchedulePlan.NextTime
		}
		return true
	})
	if nearTime == nil {
		scheduler.NearTimer.Reset(time.Second)
	} else {
		nextSchedule = nearTime.Sub(now)
		scheduler.NearTimer.Reset(nextSchedule)
	}
	return
}

/*
* 任务调度计划，把传入的jobEvent根据运行时类型做一个运行时判断
* 如果是保存任务则构建调度计划，加入到调度计划表（map）当中
* 如果是删除类型的任务则判断该任务是否在调度计划表当中存在，如果存在的话就进行删除
 */
func (scheduler *Scheduler) scheduleJobEvent(jobEvent *common.JobEvent) (err error) {
	var (
		jobSchedulePlan *common.JobSchedulePlan
		jobExecInfo     interface{}
		jobExited       bool
	)
	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE:
		//构建执行信息
		if jobSchedulePlan, err = common.BuildJobSchedulePlan(jobEvent.Job); err != nil {
			return
		}
		scheduler.JobScheduleTable.Store(jobEvent.Job.JobName, jobSchedulePlan)
	case common.JOB_EVENT_DELETE:
		//删除全局调度器的该任务调度信息
		if _, jobExited = scheduler.JobScheduleTable.Load(jobEvent.Job.JobName); jobExited {
			scheduler.JobScheduleTable.Delete(jobEvent.Job.JobName)
		}
	case common.JOB_EVENT_KILL:
		//监听到kill信息
		if jobExecInfo, jobExited = scheduler.JobExcutingTable.Load(jobEvent.Job.JobName); jobExited {
			jobExecInfo.(*common.JobExcuteInfo).Cancel()
			scheduler.JobScheduleTable.Delete(jobEvent.Job.JobName)
		}
		if _, jobExited = scheduler.JobScheduleTable.Load(jobEvent.Job.JobName); jobExited {
			scheduler.JobScheduleTable.Delete(jobEvent.Job.JobName)
		}
		fmt.Println("任务已经强行杀死，并且从正在执行队列移除")
	}
	return
}

/*
调度协程，进行推送任务的监听，强杀任务监听，执行完成信息监听和决定调度计划，每次select完毕都会重新执行调度
通过for+select关键字实现任务的多方位监听，推送任务通道，强杀任务通道，执行完成信息通道和默认超时通道
执行完毕后都要把Timer下一次通知时间设置为最新nearTime，通过timer.Reset方式进行执行
*/
func (scheduler *Scheduler) schedulerLoop() {
	var (
		jobEvent   *common.JobEvent
		err        error
		nextTime   time.Duration
		finishInfo *common.JobFinishInfo
	)
	scheduler.NearTimer = time.NewTimer(nextTime)
	for {
		scheduler.TrySchedule()
		//收到推送的事件
		select {
		//调度
		case jobEvent = <-scheduler.JobEventChan:
			if err = scheduler.scheduleJobEvent(jobEvent); err != nil {
				continue
			}
		//处理完成信息
		case finishInfo = <-scheduler.JobFinishInfo:
			scheduler.handleResult(finishInfo)
		//任务到期，要再次执行，同时要重新计算定时器
		case <-scheduler.NearTimer.C:
		}
	}
}
func (scheduler *Scheduler) handleResult(result *common.JobFinishInfo) {
	var (
		jobLog *common.JobLog
	)
	scheduler.JobExcutingTable.Delete(result.JobExcuteInfo.Job.JobName)
	if result.Err == nil {
		jobLog = &common.JobLog{
			IP:              G_config.IP,
			JobName:         result.JobExcuteInfo.Job.JobName,
			Command:         result.JobExcuteInfo.Job.Command,
			Err:             "",
			OutPut:          result.Output,
			PlanStartTime:   result.JobExcuteInfo.PlanStartTime,
			StartExcuteTime: result.JobExcuteInfo.StartExcuteTime,
			EndExcuteRTime:  result.JobExcuteInfo.EndExcuteRTime,
		}
	} else {
		jobLog = &common.JobLog{
			IP:      G_config.IP,
			JobName: result.JobExcuteInfo.Job.JobName,
			Command: result.JobExcuteInfo.Job.Command,
			OutPut:  result.Output,
			Err:     result.Err.Error(),
		}
	}
	fmt.Println(time.Now(), "  ", G_config.IP, ":", jobLog.OutPut)
	G_LogSink.Append(jobLog)
}

// 任务推送通道，jobmgr通过全局调度器进行任务推送
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.JobEventChan <- jobEvent
}

// 处理结果通道，执行器通过全局调度器进行任务推送
func (scheduler *Scheduler) PushResult(result *common.JobFinishInfo) {
	scheduler.JobFinishInfo <- result
}

// 初始化调度器，包括任务表，正在执行表，
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		JobEventChan:     make(chan *common.JobEvent),
		JobFinishInfo:    make(chan *common.JobFinishInfo),
		JobScheduleTable: sync.Map{}, /*make(map[string]*common.JobSchedulePlan)*/
		JobExcutingTable: sync.Map{}, /*make(map[string]*common.Job)*/
	}
	go G_scheduler.schedulerLoop()
	return
}
