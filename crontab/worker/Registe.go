package worker

import (
	"Distributed-task-scheduling-system/crontab/common"
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
)

var G_Register *Register

type Register struct {
	Client *clientv3.Client
	Kv     clientv3.KV
	Lease  clientv3.Lease
	IP     string
}

//	func getIpv4() (ipv4 string, err error) {
//		var (
//			addrs   []net.Addr
//			addr    net.Addr
//			IPNet   *net.IPNet
//			isIpNet bool
//		)
//		if addrs, err = net.InterfaceAddrs(); err != nil {
//			return
//		}
//		for _, addr = range addrs {
//			if IPNet, isIpNet = addr.(*net.IPNet); isIpNet && !IPNet.IP.IsLoopback() && IPNet.IP.To4() != nil {
//				ipv4 = IPNet.IP.String()
//				return
//			}
//		}
//		return "", errors.New("find ipv4 failed")
//	}

func (register *Register) Regist() {
	var (
		regKey       string
		leaseResq    *clientv3.LeaseGrantResponse
		keepResqChan <-chan *clientv3.LeaseKeepAliveResponse
		keepResq     *clientv3.LeaseKeepAliveResponse
		// putResq      *clientv3.PutResponse
		ctx    context.Context
		cancel context.CancelFunc
		err    error
	)
	regKey = common.JOB_WORKER + register.IP
	for {
		if leaseResq, err = register.Lease.Grant(context.TODO(), 10); err != nil {
			goto RETRY
		}
		if keepResqChan, err = register.Lease.KeepAlive(context.TODO(), leaseResq.ID); err != nil {
			goto RETRY
		}
		ctx, cancel = context.WithCancel(context.TODO())
		if _, err = register.Kv.Put(ctx, regKey, "", clientv3.WithLease(leaseResq.ID)); err != nil {
			goto RETRY
		}
		for keepResq = range keepResqChan {
			if keepResq == nil {
				goto RETRY
			}
		}
	RETRY:
		if cancel != nil {
			cancel()
		}
	}
}
func InitRegister() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
	)
	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndPoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeOut * int(time.Millisecond)),
	}
	if client, err = clientv3.New(config); err != nil {
		return
	}
	//创建KV，Lease
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	G_Register = &Register{
		Client: client,
		Kv:     kv,
		Lease:  lease,
		IP:     G_config.IP,
	}
	go G_Register.Regist()
	return
}
