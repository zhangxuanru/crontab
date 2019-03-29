package worker

import (
	"go.etcd.io/etcd/clientv3"
	"time"
	"net"
	"github.com/zhangxuanru/crontab/common"
	"context"
)

//注册节点到etcd
type Register struct {
	client  *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
	localIp string
}

var(
	G_register *Register
)

func InitRegister() (err error)  {
	var(
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
		localIp string
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

	if localIp,err = getLocalIp(); err!=nil{
		return
	}

	//得到KV和lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	G_register = &Register{
		client:client,
		kv:kv,
		lease:lease,
		localIp:localIp,
	}

	go G_register.keepOnline()

	return
}


//获取本机网卡IP
func getLocalIp() (ip string,err error) {
	//获取所有网卡
	var(
		addrs []net.Addr
		addr net.Addr
		ipNet *net.IPNet  //IP地址
		isIpNet bool
	)
	if addrs,err = net.InterfaceAddrs();err!=nil{
		return
	}
   //取第一个非local的网卡
   for _, addr = range addrs{
	  if ipNet,isIpNet = addr.(*net.IPNet);isIpNet && !ipNet.IP.IsLoopback(){
            if ipNet.IP.To4() != nil{
				ip = ipNet.IP.String()
				return
			}
	  }
   }
   err = common.ERR_NO_LOCAL_IP_FOUND
   return
}


//注册到ETCD
func (register *Register) keepOnline()  {
   var(
      	regKey string
      	leaseGrantResp *clientv3.LeaseGrantResponse
      	leaseKeepRespChan <-chan *clientv3.LeaseKeepAliveResponse
      	leaseKeepResp *clientv3.LeaseKeepAliveResponse
	    cancelCtx   context.Context
	    cancelFunc   context.CancelFunc
      	err error
   )

   for{
	   regKey = common.JOB_WORKER_DIR+register.localIp

	   cancelFunc = nil

	   if leaseGrantResp,err = register.lease.Grant(context.TODO(),10);err!=nil{
             goto RETRY
	   }

	   //持续续租
	   if leaseKeepRespChan,err = register.lease.KeepAlive(context.TODO(),leaseGrantResp.ID);err!=nil{
	       	goto RETRY
	   }

	   cancelCtx,cancelFunc = context.WithCancel(context.TODO())
	   //写值
	  if _,err = register.kv.Put(cancelCtx,regKey,"",clientv3.WithLease(leaseGrantResp.ID));err!=nil{
	  	   goto RETRY
	  }

	  //处理续租应答
	  for{
		  select {
		  case leaseKeepResp = <-leaseKeepRespChan:
			     if leaseKeepResp == nil{
			     	 goto RETRY
				 }
		 }
	  }

   RETRY:
   	 time.Sleep(1*time.Second)
	  if cancelFunc != nil{
	       cancelFunc()
	  }
   }

}