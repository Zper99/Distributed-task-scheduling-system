package worker

import (
	"Distributed-task-scheduling-system/crontab/common"
	errors "Distributed-task-scheduling-system/crontab/errors"
	"context"

	"github.com/coreos/etcd/clientv3"
)

// 每一个string都有一个joblock结构，包含，任务名，用于获取分布式锁的kv，lease，islock，leaseid和cancelFunc
type JobLock struct {
	jobName    string
	kv         clientv3.KV
	lease      clientv3.Lease
	cancelFunc context.CancelFunc
	leaseid    clientv3.LeaseID
	isLocked   bool
}

var G_jobLock *JobLock

// 进行尝试加锁
func (jobLock *JobLock) Lock() (err error) {
	var (
		txn          clientv3.Txn
		leaseId      clientv3.LeaseID
		leaseResq    *clientv3.LeaseGrantResponse
		ctx          context.Context
		cancelFunc   context.CancelFunc
		keepResqChan <-chan *clientv3.LeaseKeepAliveResponse
		keepResq     *clientv3.LeaseKeepAliveResponse
		lockKey      string
		txnResq      *clientv3.TxnResponse
	)
	ctx, cancelFunc = context.WithCancel(context.TODO())
	jobLock.cancelFunc = cancelFunc
	//获取租约
	if leaseResq, err = jobLock.lease.Grant(context.TODO(), 10); err != nil {
		goto FAIL
	}
	leaseId = leaseResq.ID
	jobLock.leaseid = leaseId
	//续租
	if keepResqChan, err = jobLock.lease.KeepAlive(ctx, leaseId); err != nil {
		goto FAIL
	}
	//此协程在于消耗通道消息
	go func() {
		for keepResq = range keepResqChan {
			if keepResq == nil {
				return
			}
		}
	}()
	lockKey = common.JOB_LOCK_DIR + jobLock.jobName
	txn = jobLock.kv.Txn(context.TODO())
	//txn事务进行比较，如果存在
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(lockKey))
	//收到提交回复
	if txnResq, err = txn.Commit(); err != nil {
		goto FAIL
	}
	//如果不成功则回复不成功
	if !txnResq.Succeeded {
		err = errors.NewError(errors.GetLockFailed)
		goto FAIL
	}
	//成功则回复锁成功
	jobLock.isLocked = true
	return
FAIL:
	cancelFunc()
	jobLock.lease.Revoke(context.TODO(), leaseId)
	return
}

// 释放锁，如果锁已经存在则释放锁（可能自己上的锁，被别人取消了，应该加一个id，id对应的上才可以释放）
func (jobLock *JobLock) UnLock() (err error) {
	if jobLock.isLocked {
		//取消上下文，防止续租
		jobLock.cancelFunc()
		//释放租约
		_, err = jobLock.lease.Revoke(context.TODO(), jobLock.leaseid)
	}
	return
}

// ABA
// 初始化一个分布式锁器，可以写一个string，如果发现cas如果已经存在就说明上锁了
func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) (jobLock *JobLock) {
	jobLock = &JobLock{
		jobName: jobName,
		kv:      kv,
		lease:   lease,
	}
	return
}
