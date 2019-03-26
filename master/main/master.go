package main

import (
	"runtime"
	"github.com/zhangxuanru/crontab/master"
	"fmt"
	"flag"
	"time"
)

var(
	confFile string //配置文件路径
)

func initArgs()  {
    flag.StringVar(&confFile,"config","./master.json","传入配置文件")
    flag.Parse()
}

func initEnv()  {
   runtime.GOMAXPROCS(runtime.NumCPU())
}

func main()  {
	var(
		err error
	)
	//初始化参数
	initArgs()

	//初始化线程
	initEnv()

	//加载配置
	if err = master.InitConfig(confFile);err!=nil{
		goto ERR
	}

	//任务管理器
	if err = master.InitJobMgr();err!=nil{
		goto ERR
	}

	//启动API HTTP 服务
	if err = master.InitApiServer(); err!=nil{
          goto ERR
	}

	for{
       time.Sleep(1 * time.Second)
	}

	return

	ERR:
		fmt.Println(err)
}



