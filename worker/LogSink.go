package worker

import (
	"github.com/zhangxuanru/crontab/common"
	"time"
	"gopkg.in/mgo.v2"
)

//mongodb 记录日志

type LogSink struct {
	client *mgo.Session
	logCollection *mgo.Collection
	logChan chan *common.JobLog
	autoCommitChan  chan *common.BatchLog
}

var(
	G_logSink *LogSink
)

func InitLogSink() (err error)  {
     var(
     	client *mgo.Session
	 )

	client,err = mgo.Dial(G_config.MongoDbUrl)

	 G_logSink = &LogSink{
	 	client:client,
		logCollection:client.DB("cron").C("log"),
	    logChan:make(chan *common.JobLog,1000),
	    autoCommitChan:make(chan *common.BatchLog,1000),
	 }

	 //启动协程
	 go G_logSink.writeLoop()

	 return
}

//日志存储协程
func (logSink *LogSink) writeLoop() {
	var(
		log *common.JobLog
		batchLog *common.BatchLog
		commitTimer *time.Timer
	)
	for{
		select {
		case log = <-logSink.logChan:
			//批次插入日志，
			if batchLog == nil{
				batchLog = &common.BatchLog{}
				//批次超时自动提交
				commitTimer = time.AfterFunc(1*time.Second, func(logs *common.BatchLog) func(){
                   return func() {
                       logSink.autoCommitChan <- logs
				  }
				}(batchLog),
				)
			}

            //追加log
			batchLog.Logs = append(batchLog.Logs,log)
			//批次满了 就立即保存
			if len(batchLog.Logs) >= G_config.JobLogBatchSize{
                  logSink.saveLogs(batchLog)
				  batchLog = nil
			}
		}

	}
}

//批量保存日志
func (logSink *LogSink) saveLogs(log *common.BatchLog) (err error) {
	if len(log.Logs) > 0{
		logSink.logCollection.Insert(log.Logs)
	}
	return
}


