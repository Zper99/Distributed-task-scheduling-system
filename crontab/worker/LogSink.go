package worker

import (
	"Distributed-task-scheduling-system/crontab/common"
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type LogSink struct {
	client        *mongo.Client
	logCollection *mongo.Collection
	logChan       chan *common.JobLog
	logCommitChan chan *common.LogBatch
}

var G_LogSink *LogSink

// 上传日志到mongo
func (logSink *LogSink) saveLogs(logs []interface{}) {
	go logSink.logCollection.InsertMany(context.TODO(), logs)
}

// 监听协程
func (logSink *LogSink) WriteLoop() {
	var (
		log             *common.JobLog
		logBatch        *common.LogBatch
		timeoutLogBatch *common.LogBatch
	)
	for {
		select {
		//接受日志
		case log = <-logSink.logChan:
			//如果没满则新创建一个批次，避免大量请求mingo
			if logBatch == nil {
				logBatch = &common.LogBatch{}
			}
			//过期上传和TCP缓存区一样，因为afterfunc回调函数是异步的会开启子协程，不使用回调函数的函数会导致闭包捕获
			time.AfterFunc(time.Duration(time.Second), func(logBatch *common.LogBatch) func() {
				return func() {
					logSink.logCommitChan <- logBatch
				}
			}(logBatch))
			//提交满逻辑
			logBatch.Logs = append(logBatch.Logs, log)
			if len(logBatch.Logs) >= G_config.LogConfigBatchSize {
				logSink.saveLogs(logBatch.Logs)
				logBatch.Logs = nil
			}
		case timeoutLogBatch = <-logSink.logCommitChan:
			//防止重复提交
			if timeoutLogBatch == logBatch {
				logSink.saveLogs(timeoutLogBatch.Logs)
				timeoutLogBatch.Logs = nil
			}
		}
	}
}

// 初始化mongo协程
func InitLogSink() (err error) {
	var (
		serverAPI *options.ServerAPIOptions
		opts      *options.ClientOptions
		client    *mongo.Client
	)
	serverAPI = options.ServerAPI(options.ServerAPIVersion1)
	opts = options.Client().ApplyURI(G_config.MongoDbURL).SetServerAPIOptions(serverAPI)
	if client, err = mongo.Connect(context.TODO(), opts); err != nil {
		fmt.Println("连接错误，请重试·····")
		return
	}
	G_LogSink = &LogSink{
		client:        client,
		logCollection: client.Database("DTSC").Collection("log"),
		logChan:       make(chan *common.JobLog, 10000),
		logCommitChan: make(chan *common.LogBatch, 1),
	}
	go G_LogSink.WriteLoop()
	return
}

// 放入通道，避免溢出，溢出丢弃
func (logSink *LogSink) Append(log *common.JobLog) {
	select {
	case logSink.logChan <- log:
	default:
	}
}
