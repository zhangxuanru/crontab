package master

import (
	"io/ioutil"
	"encoding/json"
)

//程序配置
type Config struct {
	ApiPort int `json:"apiPort"`
	ApiReadTimeOut int `json:"apiReadTimeOut"`
	ApiWriteTimeOut int `json:"apiWriteTimeOut"`
}
//配置单例
var (
    G_config *Config
)

//加载配置
func InitConfit(file string) (err error) {
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


