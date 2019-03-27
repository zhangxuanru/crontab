package worker

import (
	"go.etcd.io/etcd/clientv3"
	"context"
	"github.com/zhangxuanru/crontab/common"
)


//分布式锁
type JobLock struct {
    Kv clientv3.KV
    Lease  clientv3.Lease
    JobName string
	CancelFunc context.CancelFunc
	LeaseID clientv3.LeaseID
	isLock   bool
}


//初始化一把锁
func InitJobLock(jobName string,kv clientv3.KV,lease clientv3.Lease)(jobLock *JobLock)  {
  jobLock = &JobLock{
		Kv:kv,
		Lease:lease,
		JobName:jobName,
  }
	return
}


//尝试上锁
func(jobLock *JobLock) TryLock() (err error){
	  /*
	  1.创建租约（5秒）
	  2.自动续租
	  3.创建事务（txn）
	  4.事务抢锁
	  5.成功返回，失败则释放
	  */
	var(
		leaseGrantResp *clientv3.LeaseGrantResponse
		cancelTxt  context.Context
		cancelFunc context.CancelFunc
		leaseId    clientv3.LeaseID
		keepLeaseChan <-chan  *clientv3.LeaseKeepAliveResponse
		txn clientv3.Txn
		lockKey string
		txnResp  *clientv3.TxnResponse
	)
	  if leaseGrantResp,err = jobLock.Lease.Grant(context.TODO(),5);err!=nil{
		 return
	  }
	 cancelTxt,cancelFunc = context.WithCancel(context.TODO())

	 leaseId = leaseGrantResp.ID

	 if keepLeaseChan,err = jobLock.Lease.KeepAlive(cancelTxt,leaseId);err!=nil{
		goto FAIL
	 }

	 go func() {
		var(
			keepResp *clientv3.LeaseKeepAliveResponse
		)
		for{
			select {
			case keepResp = <-keepLeaseChan:  //自动续租的应答
				 if keepResp == nil{
					  goto END
				 }
			  }
		}
		 END:
	 }()

	txn = jobLock.Kv.Txn(context.TODO())
	lockKey = common.JOB_LOCK_DIR+jobLock.JobName
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey),"=",0)).Then(clientv3.OpPut(lockKey,"",clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(lockKey))

	if txnResp,err = txn.Commit();err!=nil{
		goto FAIL
	}
	if !txnResp.Succeeded{
		err = common.ERR_LOCK_EXISTS_ERROR
	    goto FAIL
	}

	jobLock.LeaseID = leaseId
	jobLock.CancelFunc = cancelFunc
	jobLock.isLock = true
  return

FAIL:
	cancelFunc() //	取消自动续租
	jobLock.Lease.Revoke(context.TODO(),leaseId)  //释放租约
    return
}


//释放锁
func (jobLock *JobLock) Unlock()  {
	if jobLock.isLock{
	     jobLock.CancelFunc() //	取消自动续租
	     jobLock.Lease.Revoke(context.TODO(),jobLock.LeaseID)  //释放租约
	}
}



