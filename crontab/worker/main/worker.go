package main

import (
	"Distributed-task-scheduling-system/crontab/worker"
	"flag"
	"fmt"
	"runtime"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

// 读取配置文件或者控制台参数
func initArgs() (err error) {
	var (
		configPath string
	)
	flag.StringVar(&configPath, "config", "./worker.json", "Worker's config path")
	if err = worker.InitConfig(configPath); err != nil {
		return
	}
	return
}

// 初始化配置，调度器，执行器，任务管理器
func main() {
	var (
		close chan bool
		err   error
	)
	close = make(chan bool)
	if err = initArgs(); err != nil {
		goto ERR
	}
	if err = worker.InitLogSink(); err != nil {
		goto ERR
	}
	if err = worker.InitScheduler(); err != nil {
		goto ERR
	}
	if err = worker.InitExcutor(); err != nil {
		goto ERR
	}
	if err = worker.InitJobMgr(); err != nil {
		goto ERR
	}
	if err = worker.InitRegister(); err != nil {
		goto ERR
	}
	fmt.Println()
	fmt.Println("------Start Listen-----")
	<-close
ERR:
	fmt.Println(err.Error())
}
