package master

import (
	"encoding/json"
	"os"
)

var G_config *Config

type Config struct {
	ListenPort    int      `json:"ApiPort"`
	ReadTimepOut  int      `json:"apiReadTimeOut"`
	WriteTimeOut  int      `json:"apiWriteTimeOut"`
	EtcdEndPoints []string `json:"etcdEndPoints"`
	EtcdDialTime  int      `json:"etcdDialTime"`
}

func InitConfig(filePath string) (err error) {
	var (
		content []byte
		conf    *Config
	)
	if content, err = os.ReadFile(filePath); err != nil {
		return
	}
	conf = &Config{}
	if err = json.Unmarshal(content, conf); err != nil {
		return
	}
	G_config = conf
	return
}
