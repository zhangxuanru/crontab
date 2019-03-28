package worker

import (
	"github.com/zhangxuanru/crontab/common"
	"time"
	"fmt"
)

//任务调度
type Scheduler struct {
	jobEventChan chan *common.JobEvent
	jobPlanTable map[string]*common.JobSchedulePlan
	JobExecutingTable map[string]*common.JobExecuteInfo   //正在执行的任务表
	jobExecutorResultChan  chan *common.JobExecuteResult
}

var(
   G_scheduler *Scheduler
)


//处理任务事件
func (scheduler *Scheduler) handleJobEvent(jobEvent *common.JobEvent)  {
	var(
		jobSchedulePlan *common.JobSchedulePlan
		jobExecuteInfo *common.JobExecuteInfo
		jobIsExecute  bool
		jobExists bool
		err error
	)
	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE:
		if jobSchedulePlan,err = common.BuildJobSchedulePlan(jobEvent.Job);err!=nil{
           return
		}
		scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan
	case common.JOB_EVENT_DELETE:
         if jobSchedulePlan,jobExists = scheduler.jobPlanTable[jobEvent.Job.Name];jobExists{
         	  delete(scheduler.jobPlanTable,jobEvent.Job.Name)
		 }
	case common.JOB_EVENT_KILLER:
		if jobExecuteInfo,jobIsExecute = scheduler.JobExecutingTable[jobEvent.Job.Name];jobIsExecute{
			jobExecuteInfo.CancelFunc()  //强杀任务
		}
	}
}

//处理任务执行结果
func (scheduler *Scheduler) handleJobResult(result *common.JobExecuteResult)  {
	var(
		jobLog *common.JobLog
	)
	//删除执行状态
   delete(scheduler.JobExecutingTable,result.ExecuteInfo.Job.Name)

   //记录执行日志
   if result.Err != common.ERR_LOCK_EXISTS_ERROR{
	   jobLog = &common.JobLog{
            JobName:result.ExecuteInfo.Job.Name,
            Command:result.ExecuteInfo.Job.Command,
            OutPut:string(result.Output),
            PlanTime:result.ExecuteInfo.PlanTime.UnixNano()/1000/1000,
            SchedulerTime:result.ExecuteInfo.RealTime.UnixNano()/1000/1000,
            StartTime:result.StartTime.UnixNano()/1000/1000,
            EndTime:result.EndTime.UnixNano()/1000/1000,
	   }
	   if result.Err != nil{
	        jobLog.Err = result.Err.Error()
	   }
	   //todo存储到mongodb

   }


   fmt.Println("执行结果:",result.ExecuteInfo.Job.Name,"output:",result.Output,"error:",result.Err)

}


//重新计算任务调度状态
func (scheduler *Scheduler) TrySchedule() (scheduleAfter time.Duration) {
	var(
		jobPlan  *common.JobSchedulePlan
		now time.Time
		nearTime *time.Time
	)

	if len(scheduler.jobPlanTable) == 0{
		scheduleAfter = 1 *time.Second
		return
	}

	now = time.Now()
   //1:遍历所有任务
    for _,jobPlan = range scheduler.jobPlanTable{
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now){
			//任务到期，尝试执行任务
			scheduler.TryStartJob(jobPlan)
			jobPlan.NextTime = jobPlan.Expr.Next(now) //更新下次执行时间
		}

		//统计最近一个要过期的任务时间
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime){
			nearTime = &jobPlan.NextTime
		}
	}

	//下次调度间隔（最近要执行的任务时间 - 当前时间）
	scheduleAfter = (*nearTime).Sub(now)
	return

   //2:过期的任务立即执行

   //3:统计最近要过期的任务的时间(scheduleAfter = N秒后过期的任务)

}

//尝试执行任务
func (scheduler *Scheduler) TryStartJob(plan *common.JobSchedulePlan)  {
	 var(
	 	 jobExecuteInfo *common.JobExecuteInfo
	 	 jobExecuteStatus bool
		)
	if  jobExecuteInfo,jobExecuteStatus = scheduler.JobExecutingTable[plan.Job.Name];jobExecuteStatus{
		  fmt.Println("尚未退出，跳过执行")
		 return
	}
	jobExecuteInfo = common.BuildJobExecuteInfo(plan)
	scheduler.JobExecutingTable[plan.Job.Name] = jobExecuteInfo
	//执行任务
	G_executor.ExecuteJob(jobExecuteInfo)
}


//调度协程
func (scheduler *Scheduler) scheduleLoop() {
	//定时任务
	var(
		jobEvent *common.JobEvent
		scheduleAfter  time.Duration
		scheduleTimer  *time.Timer
		jobResult *common.JobExecuteResult
	)

    //初始化运行
	scheduleAfter = scheduler.TrySchedule()

	//调度的定时器
	scheduleTimer = time.NewTimer(scheduleAfter)

	for{
		select {
		case  jobEvent = <-scheduler.jobEventChan:  //监听任务变化事件
               scheduler.handleJobEvent(jobEvent)
		case <-scheduleTimer.C:
		case jobResult = <-scheduler.jobExecutorResultChan:
               scheduler.handleJobResult(jobResult)
		}
		scheduleAfter = scheduler.TrySchedule()
		scheduleTimer.Reset(scheduleAfter)
	}
}


//推送任务变化事件
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}

//回传任务执行结果
func (scheduler *Scheduler) PushJobExecutorResult(result *common.JobExecuteResult) {
      scheduler.jobExecutorResultChan <- result
}



//初始化调度器
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		jobEventChan:make(chan *common.JobEvent,1000),
		jobPlanTable:make(map[string]*common.JobSchedulePlan),
		JobExecutingTable:make(map[string]*common.JobExecuteInfo),
		jobExecutorResultChan:make(chan *common.JobExecuteResult,1000),
	}
	//启动调度协程
	go G_scheduler.scheduleLoop()
	return
}
