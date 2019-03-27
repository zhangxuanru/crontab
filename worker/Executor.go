package worker

import (
	"github.com/zhangxuanru/crontab/common"
	"os/exec"
	"time"
	"math/rand"
)

//任务执行器
type Executor struct {

}

var(
	G_executor *Executor
)

//执行一个任务
func (executor *Executor) ExecuteJob(info *common.JobExecuteInfo) {
	go func() {
		var(
			cmd *exec.Cmd
			output []byte
			err error
			result *common.JobExecuteResult
			jobLock *JobLock
		)
		result = &common.JobExecuteResult{
                ExecuteInfo:info,
                Output:make([]byte,0),
                StartTime:time.Now(),
		}

		//随机睡眠时间
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

		//获取分布式锁， 如果没锁上，则不执行
		jobLock = G_jobMgr.CreateJobLock(info.Job.Name)
		err = jobLock.TryLock()
		defer jobLock.Unlock()
		if err!=nil{
			result.Err = err
			result.EndTime = time.Now()
			//执行结果返回 scheduler
			G_scheduler.PushJobExecutorResult(result)
			return
		}

		result.StartTime = time.Now()
        //执行shell命令
		cmd = exec.CommandContext(info.Context,"/bin/bash","-c",info.Job.Command)
		output,err = cmd.CombinedOutput()

		result.EndTime = time.Now()
		result.Output = output
		result.Err = err
		//执行结果返回 scheduler
		G_scheduler.PushJobExecutorResult(result)
	}()

}


//初始化执行器
func InitExecutor() (err error) {
    G_executor = &Executor{}
    return
}
