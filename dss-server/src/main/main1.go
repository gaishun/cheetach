package main

import (
	"../bitstore"
	"encoding/json"
	"google.golang.org/grpc"
	"io/ioutil"
	"log"
	"net"
	"os"
	"syscall"
)

type Config struct {
	Addr string
	Block_device string
	Logdir string
}

func Load (filename string  , v interface{}){
	//ReadFile函数会读取文件的全部内容，并将结果以[]byte类型返回
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return
	}
	//读取的数据为json格式，需要进行解码
	err = json.Unmarshal(data, v)
	if err != nil {
		return
	}
}

func main (){
	//定义配置文件地址
	var file = "../conf/conf1.json"
	//定义一个设备
	var block_device = new(bitstore.Block_device_t)
	var err error

	//加上分析文件的内容，把文件内容解析出来
	//设置好services.TempPort，设置好block_device.fd
	var conf Config
	Load(file, &conf)
	log.Println(conf.Addr)
	log.Println(conf.Logdir)
	log.Println(conf.Block_device)
	bitstore.TempPort = conf.Addr
	bitstore.Logdir = conf.Logdir

	//打开设备
	block_device.Fd_direct, err = syscall.Open(conf.Block_device,os.O_RDWR,0066)
	if err!=nil{
		log.Fatal("OPEN_DEVICE_FAILE %v\n",conf.Block_device)
		return
	}

	//设置监听ip和端口
	lis , err := net.Listen("tcp" , bitstore.TempPort)
	if err != nil {
		log.Fatal("Failed to listen: %v" , bitstore.TempPort)
	}
	//新建一个server
	s := grpc.NewServer()
	//初始化server
	bitstore.RegisterBitStoreServer(s,&bitstore.BitStoreService{
		Block_device: block_device,
	})
	//启动服务
	if err := s.Serve(lis) ; err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	log.Println("SUCCESS start service at %v", bitstore.TempPort)
}
