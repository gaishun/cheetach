package config

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"lib/glog"
	"net/http"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	CLEARINTERNAL = 3600
)

/*
 * name :			monitorConfigDB
 * Description :	打开一个协程监控DB, 60秒检查一次。
 * input :
 * output :
 * return :
 * creator :		HuangChunhua
 * creat date :		2017-07-25
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func monitorConfigDB(dbAddr string) {
	var mutex *sync.Mutex = new(sync.Mutex)
	go func() {
		for {
			time.Sleep(time.Duration(60) * time.Second)
			mutex.Lock()
			//one modification would trigger more than one events, this is to merge the events
			data, err := loadFromDB(dbAddr)
			if err != nil {
				glog.Errorf("error occurs when loading config periodly, error is %v", err)
			} else if data == nil {
				glog.Info("no need to update\n")
			} else { // err == nil && data s not empty
				rwMutex.Lock()
				configData = data
				rwMutex.Unlock()
				glog.Info("load config successful")

			}
			mutex.Unlock()
		}
	}()
}

/*
 * name :			monitorFsCapacity
 * Description :	监控gift-fs的容量
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-09-05
 * correcter :
 * correct date :
 * reason :
 * version :	2.1.0.0
 */
type Pretty struct {
	Topology map[string]interface{} `json:"topology"`
	Version  string                 `json:"version"`
}

type ClearDir struct {
	Dir        string
	ExpireTime int64
}

/*
 * name :			LogClearDaemon
 * Description :	定时清除过期日志和tmp文件
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-09-15
 * correcter :
 * correct date :
 * reason :
 * version :	2.1.2.0
 */
func LogClearDaemon(dirs []ClearDir) {
	dirClear := func(dir ClearDir) {
		files, err := ioutil.ReadDir(dir.Dir)
		if err != nil {
			return
		}
		for _, file := range files {
			if time.Now().Unix()-file.ModTime().Unix() >= dir.ExpireTime {
				os.Remove(dir.Dir + "/" + file.Name())
			}
		}
	}

	tickChan := time.Tick(CLEARINTERNAL * time.Second)
	for {
		select {
		case <-tickChan:
			for _, dir := range dirs {
				dirClear(dir)
			}
		}
	}
}

type ClusterStatusResult struct {
	IsLeader bool     `json:"IsLeader,omitempty"`
	Leader   string   `json:"Leader,omitempty"`
	Peers    []string `json:"Peers,omitempty"`
}

func decodeJson(r io.Reader, v interface{}) error {
	return json.NewDecoder(r).Decode(v)
}
func GetClusterLeader(addr string) (Leader string, err error) {
	u := url.URL{
		Scheme: "http",
		Host:   addr,
		Path:   "/cluster/status",
	}
	c := http.Client{
		Timeout: 300 * time.Millisecond,
	}
	resp, err := c.Get(u.String())
	if err != nil {
		return
	}

	defer resp.Body.Close()

	status := new(ClusterStatusResult)
	if err = decodeJson(resp.Body, status); err != nil {
		return
	}
	Leader = status.Leader

	return
}

func monitorFS() {
	go func() {
		for {
			var err error
			FileStorage := RequireConfigData().FileStorage
			for fsid, val := range FileStorage {
				for i := 0; i < len(val.Addr); i++ {
					if _, err = GetClusterLeader(val.Addr[i].MasterAddr); err != nil {
						atomic.StoreInt32(&FileStorage[fsid].Addr[i].Status, 0)
					} else {
						atomic.StoreInt32(&FileStorage[fsid].Addr[i].Status, 1)
					}
				}
			}
			glog.Info("update fs status\n")
			time.Sleep(time.Duration(10) * time.Second)
		}
	}()
}
