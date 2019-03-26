package common

import (
	"encoding/json"
	"strings"
	"github.com/gorhill/cronexpr"
	"time"
)

//定时任务
type Job struct {
    Name string  `json:"name"`           //任务名
    Command string `json:"command"`      //shell命令
    CronExpr string `json:"cronExpr"`     //cron 表达式
}

//任务调度计划
type JobSchedulePlan struct {
    Job *Job
    Expr *cronexpr.Expression //解析好的cronexpr表达式
    NextTime time.Time //下次调度时间
}



//HTTP 接口应答
type Response struct {
	Errno int `json:"errno"`
	Msg string `json:"msg"`
	Data interface{} `json:"data"`
}

//变化事件
type JobEvent struct {
	EventType int //save  delete
	Job *Job
}


//应答方法
func BuildResponse(errno int,msg string,data interface{}) (resp []byte,err error) {
      var(
      	response Response
	  )
	response.Errno = errno
	response.Msg = msg
	response.Data = data

	resp,err = json.Marshal(response)
	return
}


//反序列化JOB
func UnpackJob(value []byte) (ret *Job,err error)  {
	var(
		job *Job
	)
	job = &Job{}
    if err = json.Unmarshal(value,job);err!=nil{
    	return
	}
	ret = job
	return
}

//提取任务名
func ExtractJobName(jobKey string)(string)  {
      return strings.TrimPrefix(jobKey,JOB_SAVE_DIR)
}


//任务变化事件
func BuildJobEvent(eventType int,job *Job)(jobEvent *JobEvent)  {
	return &JobEvent{
		EventType:eventType,
		Job:job,
	}
}

//构造执行计划
func BuildJobSchedulePlan(job *Job)(jobSchedulePlan *JobSchedulePlan,err error)  {
	var(
		expr *cronexpr.Expression
	)
	if expr,err = cronexpr.Parse(job.CronExpr);err!=nil{
       return
	}
	jobSchedulePlan = &JobSchedulePlan{
          Job:job,
          Expr:expr,
          NextTime:expr.Next(time.Now()),
	}
	return
}
