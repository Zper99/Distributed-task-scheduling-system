package main

import (
	master "Distributed-task-scheduling-system/crontab/master"
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/sirupsen/logrus"
)

var (
	configPath string
)

func init() {
	logrus.SetFormatter(&logrus.JSONFormatter{}) //设置日志为json格式
	logrus.SetOutput(os.Stdout)
	logrus.SetLevel(logrus.InfoLevel)
	runtime.GOMAXPROCS(runtime.NumCPU())
}
func initConfig() (err error) {
	flag.StringVar(&configPath, "config", "./master.json", "master的配置文件路径")
	if err = master.InitConfig(configPath); err != nil {
		return
	}
	return
}

// 初始化配置文件，初始化任务管理器，初始化API网关
func main() {
	var (
		err  error
		over <-chan bool
	)
	over = make(chan bool, 1)
	if err = initConfig(); err != nil {
		goto ERR
	}
	if err = master.InitJobMgr(); err != nil {
		goto ERR
	}
	if err = master.InitApiServer(); err != nil {
		goto ERR
	}
	fmt.Print("\n\n")
	fmt.Println("-----welcome to Distributed-task-scheduling-system-----")
	fmt.Printf("-x-x-x-Listening on port %d-x-x-x-\n\n", master.G_config.ListenPort)
	<-over
ERR:
	fmt.Println(err)
}
