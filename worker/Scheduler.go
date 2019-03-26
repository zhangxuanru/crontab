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
}

var(
   G_scheduler *Scheduler
)


//处理任务事件
func (scheduler *Scheduler) handleJobEvent(jobEvent *common.JobEvent)  {
	var(
		jobSchedulePlan *common.JobSchedulePlan
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
	}
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
			fmt.Println("run:",jobPlan.Job.Name)


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



//调度协程
func (scheduler *Scheduler) scheduleLoop() {
	//定时任务
	var(
		jobEvent *common.JobEvent
		scheduleAfter  time.Duration
		scheduleTimer  *time.Timer
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
		}
		scheduleAfter = scheduler.TrySchedule()
		scheduleTimer.Reset(scheduleAfter)
	}
}


//推送任务变化事件
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}




//初始化调度器
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		jobEventChan:make(chan *common.JobEvent,1000),
		jobPlanTable:make(map[string]*common.JobSchedulePlan),
	}
	//启动调度协程
	go G_scheduler.scheduleLoop()
	return
}
