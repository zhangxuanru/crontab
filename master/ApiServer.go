package master

import (
	"net/http"
	"net"
	"time"
	"strconv"
	"github.com/zhangxuanru/crontab/common"
	"encoding/json"
)

//任务的HTTP接口
type ApiServer struct {
     httpServer *http.Server
}

var(
	G_apiServer *ApiServer
)

//保存任务接口，任务保存到ETCD
func handleJobSave(w http.ResponseWriter, r *http.Request){
	 var(
	 	err error
	 	postJob string
	 	job common.Job
	 	oldJob *common.Job
	 	bytes []byte
	 )
	 if err = r.ParseForm();err!=nil{
	 	goto ERR
	 }

	 postJob = r.PostForm.Get("job")

	 if err = json.Unmarshal([]byte(postJob),&job);err!=nil{
          goto ERR
	 }

	 //save
	if oldJob,err = G_jobMgr.SaveJob(job);err!=nil{
       goto ERR
	}

	if bytes,err = common.BuildResponse(0,"success",oldJob); err==nil{
         w.Write(bytes)
	}
	return
 ERR:
	 if bytes,err = common.BuildResponse(-1,err.Error(),nil); err==nil{
		 w.Write(bytes)
	 }
}


//删除任务接口
func handleJobDelete(w http.ResponseWriter, r *http.Request)  {
	var(
		err error
		name string
		oldJob *common.Job
		bytes []byte
	)
	 r.ParseForm()
	 name = r.PostForm.Get("name")
	 if oldJob,err =  G_jobMgr.DeleteJob(name);err!=nil{
         goto ERR
	 }
	if bytes,err = common.BuildResponse(0,"success",oldJob);err==nil{
		 w.Write(bytes)
	}
	return
ERR:
	if bytes,err = common.BuildResponse(-1,err.Error(),nil);err==nil{
		w.Write(bytes)
	}
}

//列举任务列表
func handleJobList(w http.ResponseWriter, r *http.Request)  {
	var(
		jobList []*common.Job
		err error
		bytes []byte
	)
	if jobList,err = G_jobMgr.ListJobs();err!=nil{
		goto ERR
	}

	if bytes,err = common.BuildResponse(0,"success",jobList);err==nil{
		w.Write(bytes)
	}
ERR:
	if bytes,err = common.BuildResponse(-1,err.Error(),nil);err==nil{
		w.Write(bytes)
	}
}





//初始化服务
func InitApiServer() (err error) {
	 //配置路由
	 var(
	 	mux  *http.ServeMux
	 	listener net.Listener
	 )

    //配置路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save",handleJobSave)
	mux.HandleFunc("/job/delete",handleJobDelete)
	mux.HandleFunc("/job/list",handleJobList)

    //启动TCP监听handleJobDelete
	if listener,err = net.Listen("tcp",":"+strconv.Itoa(G_config.ApiPort)); err!=nil{
		return
	}

	httpServer := &http.Server{
		ReadTimeout: time.Duration(G_config.ApiReadTimeOut) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.ApiWriteTimeOut) * time.Millisecond,
		Handler:mux,
	}

	//赋值单例
	G_apiServer = &ApiServer{
		 httpServer:httpServer,
	}

	//启动了服务端
	go httpServer.Serve(listener)

	return
}



