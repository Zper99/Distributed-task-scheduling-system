package worker

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
)

type Config struct {
	IP                 string
	EtcdEndPoints      []string `json:"etcdEndPoints"`
	EtcdDialTimeOut    int      `json:"etcdDialTimeOut"`
	MongoDbURL         string   `json:"mongoDbURL"`
	MongoConnTimeout   int      `json:"mongo_connect_timeout"`
	LogConfigBatchSize int      `json:"log_batch_size"`
	RandomSleepTime    int      `json:"random_sleep"`
}

var G_config *Config

func getIpv4() (ipv4 string, err error) {
	resp, err := http.Get("https://api.ipify.org?format=text")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}
func InitConfig(filePath string) (err error) {
	var (
		bytes  []byte
		config *Config
		ipv4   string
	)
	if bytes, err = os.ReadFile(filePath); err != nil {
		return
	}
	config = &Config{}
	if err = json.Unmarshal(bytes, config); err != nil {
		return
	}
	G_config = config
	if ipv4, err = getIpv4(); err != nil {
		return
	}
	G_config.IP = ipv4
	return
}
