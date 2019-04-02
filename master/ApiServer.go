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
	return
ERR:
	if bytes,err = common.BuildResponse(-1,err.Error(),nil);err==nil{
		w.Write(bytes)
	}
	return
}

//杀死任务
func handleJobKil(w http.ResponseWriter,r *http.Request)  {
	var (
		jobName string
		err error
		bytes []byte
	)
   r.ParseForm()
   jobName =  r.PostForm.Get("name")
   if err = G_jobMgr.KillJob(jobName);err!=nil{
     	goto ERR
   }
  if bytes,err = common.BuildResponse(0,"success",nil);err==nil{
		w.Write(bytes)
	}
	return
 ERR:
	if bytes,err = common.BuildResponse(-1,err.Error(),nil);err==nil{
		w.Write(bytes)
	}
 }


 //查看日志接口
func handleJobLog(w http.ResponseWriter, r *http.Request)  {
	var(
		err error
		name string
		skipParam string
		limitParam string
		skip int
		limit int
		logArr []*common.JobLog
		bytes []byte
	)
	r.ParseForm()
	name = r.Form.Get("name")
	skipParam = r.Form.Get("skip")
	limitParam = r.Form.Get("limit")
	if skip,err = strconv.Atoi(skipParam);err!=nil{
		skip = 0
	}
	if limit,err = strconv.Atoi(limitParam);err!=nil{
		limit = 20
	}
	if logArr,err = G_logMgr.ListLog(name,skip,limit);err!=nil{
		goto ERR
	}
	if bytes,err = common.BuildResponse(0,"success",logArr);err==nil{
		w.Write(bytes)
	}
	return
	ERR:
		if bytes,err = common.BuildResponse(-1,err.Error(),nil);err==nil{
			w.Write(bytes)
		}

}

//获取健康节点
func handleWorkerList(w http.ResponseWriter, r *http.Request)  {
	var(
		workerArr []string
		err error
		bytes []byte
	)
	if workerArr,err = G_WorkerMgr.getWorkerList();err!=nil{
		 goto ERR
	}
	if bytes,err = common.BuildResponse(0,"success",workerArr);err==nil{
		w.Write(bytes)
	}
	return
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
		staticHandler http.Handler
	 )

    //配置路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save",handleJobSave)
	mux.HandleFunc("/job/delete",handleJobDelete)
	mux.HandleFunc("/job/list",handleJobList)
	mux.HandleFunc("/job/kill",handleJobKil)
	mux.HandleFunc("/job/log",handleJobLog)
	mux.HandleFunc("/worker/list",handleWorkerList)

	staticHandler = http.FileServer(http.Dir(G_config.WebRoot))
	mux.Handle("/",http.StripPrefix("/",staticHandler))

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



