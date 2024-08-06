package master

import (
	common "Distributed-task-scheduling-system/crontab/common"
	errors "Distributed-task-scheduling-system/crontab/errors"
	"context"
	"encoding/json"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

var G_jobMgr *JobMgr

type JobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

// 配置etcd连接信息和客户端，创建KV和Lease接口实例，初始化JobMgr结构体
func InitJobMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
		jobMgr *JobMgr
	)
	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndPoints,
		DialTimeout: time.Duration(G_config.EtcdDialTime) * time.Millisecond,
	}
	if client, err = clientv3.New(config); err != nil {
		return
	}
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}
	G_jobMgr = jobMgr
	return
}

// 向etcd保存任务，如果有任务则覆盖，并且返回覆盖的任务
func (jobMgr *JobMgr) SaveJob(job *common.Job) (oldjob *common.Job, err error) {
	var (
		jobKey  string
		jobVal  []byte
		putResp *clientv3.PutResponse
	)
	jobKey = common.JOB_DIR + job.JobName
	if jobVal, err = json.Marshal(job); err != nil {
		return
	}
	if putResp, err = jobMgr.kv.Put(context.TODO(), jobKey, string(jobVal), clientv3.WithPrevKV()); err != nil {
		return
	}
	if putResp.PrevKv != nil {
		oldjob = &common.Job{}
		err = json.Unmarshal(putResp.PrevKv.Value, oldjob)
	}
	return
}

// 向etcd删除任务，如果存在则返回旧任务
func (jobMgr *JobMgr) DeleteJob(job *common.Job) (delJob *common.Job, err error) {
	var (
		delResq *clientv3.DeleteResponse
		jobKey  string
	)
	jobKey = common.JOB_DIR + job.JobName
	if delResq, err = jobMgr.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}
	if delResq.PrevKvs != nil {
		delJob = &common.Job{}
		err = json.Unmarshal(delResq.PrevKvs[0].Value, delJob)
	} else {
		err = errors.NewError(errors.JobNoExitError)
	}
	return
}

// 列出所有的任务列表，range遍历get返回的所有json信息，然后序列化到job数组
func (jobMgr *JobMgr) ListAllJob() (allJob []*common.Job, err error) {
	var (
		getResq *clientv3.GetResponse
		jobPair *mvccpb.KeyValue
		job     *common.Job
		jobKey  string
	)
	jobKey = common.JOB_DIR
	if getResq, err = jobMgr.kv.Get(context.TODO(), jobKey, clientv3.WithPrefix()); err != nil {
		return
	}
	allJob = make([]*common.Job, 0)
	if len(getResq.Kvs) != 0 {
		for _, jobPair = range getResq.Kvs {
			job = &common.Job{}
			if err = json.Unmarshal(jobPair.Value, job); err != nil {
				return
			}
			allJob = append(allJob, job)
		}
	}
	return
}

// 强制杀死job，先检测有无内容，然后把内容放置为空，再带上过期时间
func (jobMgr *JobMgr) KillJob(jobName string) (err error) {
	var (
		leaseResq *clientv3.LeaseGrantResponse
		getResq   *clientv3.GetResponse
		jobKey    string
		leaseId   clientv3.LeaseID
	)
	jobKey = common.JOB_DIR + jobName
	if getResq, err = jobMgr.kv.Get(context.TODO(), jobKey); err != nil {
		return
	}
	if getResq.Kvs == nil {
		err = errors.NewError(errors.KillJobError)
		return
	}
	if leaseResq, err = jobMgr.lease.Grant(context.TODO(), 2); err != nil {
		return err
	}
	leaseId = leaseResq.ID
	jobKey = common.JOB_KILLER_DIR + jobName
	if _, err = jobMgr.kv.Put(context.TODO(), jobKey, "", clientv3.WithLease(leaseId)); err != nil {
		return
	}
	return
}

// 强制杀死job，先检测有无内容，然后把内容放置为空，再带上过期时间
func (jobMgr *JobMgr) GetHealthWorker() (healthWorkers []string, err error) {
	var (
		getResq *clientv3.GetResponse
		kv      *mvccpb.KeyValue
		worker  string
	)
	if getResq, err = jobMgr.kv.Get(context.TODO(), common.JOB_WORKER, clientv3.WithPrefix()); err != nil {
		return
	}
	healthWorkers = make([]string, 0)
	if len(getResq.Kvs) != 0 {
		for _, kv = range getResq.Kvs {
			worker = common.ExtractIP(string(kv.Key))
			healthWorkers = append(healthWorkers, worker)
		}
	}
	return
}
