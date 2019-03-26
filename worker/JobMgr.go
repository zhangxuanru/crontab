package worker

import (
	"time"
	"go.etcd.io/etcd/clientv3"
	"context"
	"github.com/zhangxuanru/crontab/common"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

//任务管理器
type JobMgr struct {
    client  *clientv3.Client
    kv  clientv3.KV
    lease clientv3.Lease
    watcher  clientv3.Watcher
} 

var(
	G_jobMgr *JobMgr
)

//初始化管理器
func InitJobMgr() (err error) {
	var(
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
		watcher clientv3.Watcher
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
	//得到KV和lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)

	//赋值单例
	G_jobMgr = &JobMgr{
		client:client,
		kv:kv,
		lease:lease,
		watcher:watcher,
	}

	//启动监听
	G_jobMgr.watchJobs()

	return
}



//监听任务变化
 func (JobMgr *JobMgr) watchJobs() (err error) {
    var(
    	getResp  *clientv3.GetResponse
    	kvPair    *mvccpb.KeyValue
    	job       *common.Job
    	watchStartRevision int64
    	watchChan     clientv3.WatchChan
    	watchResp     clientv3.WatchResponse
    	watchEvent    *clientv3.Event
    	jobName      string
    	jobEvent      *common.JobEvent
	)
   if getResp,err = G_jobMgr.kv.Get(context.TODO(),common.JOB_SAVE_DIR,clientv3.WithPrefix());err!=nil{
       return
   }
  for _,kvPair = range getResp.Kvs{
        if job,err  =  common.UnpackJob(kvPair.Value);err==nil{
			jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE,job)
			//把JOB同步给scheduler
			G_scheduler.PushJobEvent(jobEvent)
		}
  }
  go func() {
	  watchStartRevision = getResp.Header.Revision+1
      //监听/cron/jobs后续变化
	  watchChan =  G_jobMgr.watcher.Watch(context.TODO(),common.JOB_SAVE_DIR,clientv3.WithRev(watchStartRevision),clientv3.WithPrefix())
      //处理监听事件
      for watchResp = range watchChan{
		  for _,watchEvent = range watchResp.Events{
			  switch watchEvent.Type {
			  case mvccpb.PUT: //任务保存事件
				  if job,err = common.UnpackJob(watchEvent.Kv.Value); err!=nil{
					continue
				  }
				  jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE,job)
			  case mvccpb.DELETE: //任务删除事件
				  jobName = common.ExtractJobName(string(watchEvent.Kv.Key))
				  job = &common.Job{Name:jobName}
				  jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE,job)
			  }
			  //todo 推送给scheduler
			  G_scheduler.PushJobEvent(jobEvent)
		  }
	  }


  }()

  return
}



