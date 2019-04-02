package master

import (
	"go.etcd.io/etcd/clientv3"
	"time"
	"context"
	"github.com/zhangxuanru/crontab/common"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

type WorkerMgr struct {
	client *clientv3.Client
	kv clientv3.KV
}

var(
	G_WorkerMgr *WorkerMgr
)

func InitWorkerMgr()(err error)  {
	var(
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
	)
	//初始化配置
	config = clientv3.Config{
		Endpoints:G_config.EtcdEndPoints,//集群地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeOut) * time.Millisecond,
	}
	//建立连接
	if client,err = clientv3.New(config); err!=nil{
		return
	}
	kv = clientv3.NewKV(client)
	G_WorkerMgr = &WorkerMgr{
		client:client,
		kv:kv,
	}
	return
}


func (workerMgr *WorkerMgr) getWorkerList()  (workerArr []string,err error) {
	workerArr = make([]string,0)
	var(
		getResp *clientv3.GetResponse
		kv *mvccpb.KeyValue
		workerIp string
	)
	if getResp,err = workerMgr.kv.Get(context.TODO(),common.JOB_WORKER_DIR,clientv3.WithPrefix());err!=nil{
		 return
	}
	for _,kv = range getResp.Kvs{
         workerIp = common.ExtractWorkerIp(string(kv.Key))
		 workerArr = append(workerArr,workerIp)
	}
	return
}




