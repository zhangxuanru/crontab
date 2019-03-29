package master

import (
	"gopkg.in/mgo.v2"
	"github.com/zhangxuanru/crontab/common"
)

type LogMgr struct {
	client *mgo.Session
	logCollection *mgo.Collection
}

var(
	G_logMgr  *LogMgr
)

func InitLogMgr() (err error) {
	var(
		client *mgo.Session
	)
	if client,err = mgo.Dial(G_config.MongoDbUrl);err!=nil{
		 return
	}
	G_logMgr = &LogMgr{
		client:client,
		logCollection:client.DB("cron").C("log"),
	}
	return
}


//日志列表
func (logMgr *LogMgr) ListLog(jobName string,skip int,limit int) (logArr []*common.JobLog,err error)  {
    var(
		filter *common.LogFilter
	)
    filter = &common.LogFilter{
    	JobName:jobName,
	}
	logArr = make([]*common.JobLog,0)
	err = logMgr.logCollection.Find(filter).Sort("-startTime").Limit(limit).Skip(skip).All(&logArr)
	return
}
