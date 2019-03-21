package master

import (
	"net/http"
	"net"
	"time"
	"strconv"
)

//任务的HTTP接口
type ApiServer struct {
     httpServer *http.Server
}

var(
	G_apiServer *ApiServer
)

func handleJobSave(w http.ResponseWriter, r *http.Request){

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

    //启动TCP监听
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



