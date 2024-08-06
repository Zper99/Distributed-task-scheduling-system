package worker

import (
	"Distributed-task-scheduling-system/crontab/common"
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

type JobMgr struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
}

var G_jobMgr *JobMgr

// 为对应的job任务创建一个分布式锁获取器
func (jobMgr *JobMgr) creatLock(jobName string) (jobLock *JobLock) {
	return InitJobLock(jobName, jobMgr.kv, jobMgr.lease)
}

// 启动时获取对应etcd所有任务（新加入节点），然后开启一个协程通过watcher监听订阅发生变化的任务事件
func (jobMgr *JobMgr) watchJobs() (err error) {
	var (
		getResq            *clientv3.GetResponse
		kvPair             *mvccpb.KeyValue
		job                *common.Job
		watchStartRevision int64
		watchChan          clientv3.WatchChan
		watchResq          clientv3.WatchResponse
		watchEvent         *clientv3.Event
		jobName            string
		jobEvent           *common.JobEvent
	)
	//获取以该前缀的所有任务列表
	if getResq, err = jobMgr.kv.Get(context.TODO(), common.JOB_DIR, clientv3.WithPrefix()); err != nil {
		return
	}
	for _, kvPair = range getResq.Kvs {
		if job, err = common.UnPack(kvPair.Value); err != nil {
			return
		}
		//构建任务事件
		jobEvent = common.BuildJobEvent(job, common.JOB_EVENT_SAVE)
		//推送给调度器调度
		G_scheduler.PushJobEvent(jobEvent)
	}
	//监听执行保存和删除
	go func() {
		//定义监听通道获取监听事件，根据当前版本号进行监听之后的事件
		watchStartRevision = getResq.Header.Revision + 1
		watchChan = jobMgr.watcher.Watch(context.TODO(), common.JOB_DIR, clientv3.WithRev(watchStartRevision), clientv3.WithPrefix())
		//迭代通道，并且根据运行时类型来决定推送的通道，这是添加和删除，不是强杀，强杀是指把任务立刻kill -9，删除是指，不在调度列表里面再次进行调度
		for watchResq = range watchChan {
			for _, watchEvent = range watchResq.Events {
				switch watchEvent.Type {
				case mvccpb.PUT:
					if job, err = common.UnPack(watchEvent.Kv.Value); err != nil {
						continue
					}
					jobEvent = common.BuildJobEvent(job, common.JOB_EVENT_SAVE)
					G_scheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE:
					jobName = common.ExtractSave(string(watchEvent.Kv.Key))
					job = &common.Job{JobName: jobName}
					jobEvent = common.BuildJobEvent(job, common.JOB_EVENT_DELETE)
					G_scheduler.PushJobEvent(jobEvent)
				}
			}
		}
	}()
	return
}
func (jobMgr *JobMgr) watchKiller() (err error) {
	var (
		jobName    string
		job        *common.Job
		watchChan  clientv3.WatchChan
		watchResq  clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobEvent   *common.JobEvent
	)
	//监听强杀列表
	go func() {
		watchChan = jobMgr.watcher.Watch(context.TODO(), common.JOB_KILLER_DIR, clientv3.WithPrefix())
		//监听杀死任务
		for watchResq = range watchChan {
			for _, watchEvent = range watchResq.Events {
				switch watchEvent.Type {
				case mvccpb.PUT:
					jobName = common.ExtractKiller(string(watchEvent.Kv.Key))
					job = &common.Job{JobName: jobName}
					jobEvent = common.BuildJobEvent(job, common.JOB_EVENT_KILL)
					G_scheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE:
				}
			}
		}
	}()
	return
}

// 同样配置一个初始化任务调度器，进行初始化客户端，kv，lease和watcher，并且执行watch进行任务的监听
func InitJobMgr() (err error) {
	var (
		config  clientv3.Config
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		watcher clientv3.Watcher
	)
	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndPoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeOut * int(time.Millisecond)),
	}
	if client, err = clientv3.New(config); err != nil {
		return
	}
	//创建KV，Lease，Watcher
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)
	G_jobMgr = &JobMgr{
		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
	}
	//创建后，直接把还存在在etcd的任务执行一次调度
	G_jobMgr.watchJobs()
	G_jobMgr.watchKiller()
	return
}
