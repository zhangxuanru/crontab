package worker

import (
	"io/ioutil"
	"encoding/json"
)

//程序配置
type Config struct {
	EtcdEndPoints []string `json:"etcdEndPoints"`
	EtcdDialTimeOut int `json:"etcdDialTimeOut"`
}

//配置单例
var (
    G_config *Config
)

//加载配置
func InitConfig(file string) (err error) {
	var (
		content []byte
		conf Config
	)
	if content,err = ioutil.ReadFile(file); err!=nil{
		return
	}
	if err = json.Unmarshal(content, &conf);err!=nil{
		return
	}
	G_config = &conf
	return
}


