package master

import (
	"time"
	"go.etcd.io/etcd/clientv3"
	"github.com/zhangxuanru/crontab/common"
	"encoding/json"
	"context"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

//任务管理器
type JobMgr struct {
    client  *clientv3.Client
    kv  clientv3.KV
    lease clientv3.Lease
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

	//赋值单例
	G_jobMgr = &JobMgr{
		client:client,
		kv:kv,
		lease:lease,
	}
	return
}


//保存任务
func (JobMgr *JobMgr) SaveJob(job common.Job)(oldJob *common.Job, err error)  {
  //把任务保存到 /cron/jobs/任务名 ->json
  var(
  	jobKey string
  	jobValue []byte
  	putResp *clientv3.PutResponse
  	oldJobObj common.Job
  )

  //etcd保存key
  jobKey = common.JOB_SAVE_DIR+job.Name

  if jobValue,err = json.Marshal(job);err!=nil{
      return
  }

  //保存到etcd
  if putResp,err = JobMgr.kv.Put(context.TODO(),jobKey,string(jobValue),clientv3.WithPrevKV());err!=nil{
      return
  }
  //如果更新， 返回 旧值
  if putResp.PrevKv != nil{
  	   //对旧值做反序列化
  	   if err = json.Unmarshal(putResp.PrevKv.Value,&oldJobObj);err!=nil{
  	   	   err = nil
  	   	   return
	   }
	   oldJob = &oldJobObj
  }
   return
}

//删除任务
func (JobMgr *JobMgr) DeleteJob(name string) (oldJob *common.Job,err error) {
      var(
      	jobKey string
      	delResp *clientv3.DeleteResponse
	    oldJobObj common.Job
	  )
      jobKey = common.JOB_SAVE_DIR+name

      if delResp,err = JobMgr.kv.Delete(context.TODO(),jobKey,clientv3.WithPrevKV()); err!=nil{
          return
	  }
	  if len(delResp.PrevKvs) != 0{
	  	  json.Unmarshal(delResp.PrevKvs[0].Value,oldJobObj)
		  oldJob = &oldJobObj
	  }
	  return
}

//任务列表
func (JobMgr *JobMgr) ListJobs() (jobList []*common.Job,err error){
	var(
		dirKey string
		getResp  *clientv3.GetResponse
		kvPair  *mvccpb.KeyValue
		job *common.Job
	)
	dirKey = common.JOB_SAVE_DIR

	if getResp,err = JobMgr.kv.Get(context.TODO(),dirKey,clientv3.WithPrefix());err!=nil{
		return
	}

	jobList = make([]*common.Job,0)
	for _,kvPair = range getResp.Kvs{
		// job = &common.Job{}
       if err = json.Unmarshal(kvPair.Value,job);err!=nil{
		    err = nil
          	continue
	   }
		jobList = append(jobList,job)
	}
	return
}

//杀死任务
func (JobMgr *JobMgr) KillJob(jobName string) (err error) {
  var(
  	killerKey string
  	leaseGrantResp *clientv3.LeaseGrantResponse
  	leaseId clientv3.LeaseID
  )
   killerKey = common.JOB_KILLER_DIR+jobName

   if leaseGrantResp,err = JobMgr.lease.Grant(context.TODO(),1);err!=nil{
 	  return
   }
	leaseId = leaseGrantResp.ID
	if _,err = JobMgr.kv.Put(context.TODO(),killerKey,"",clientv3.WithLease(leaseId));err!=nil{
		return
	}
	return
}

