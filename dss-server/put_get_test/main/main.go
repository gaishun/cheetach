package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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

//func main (){
//	//定义配置文件地址
//	var file = "/Users/gaishun/project/DssServer/src/conf/conf.json"
//	//定义一个设备
//	var block_device = new(bitstore.Block_device_t)
//	var err error
//
//	//加上分析文件的内容，把文件内容解析出来
//	//设置好services.TempPort，设置好block_device.fd
//	var conf Config
//	Load(file, &conf)
//	bitstore.TempPort = conf.Addr
//	bitstore.Logdir = conf.Logdir
//
//	//打开设备
//	//block_device.Fd_direct, err = syscall.Open(conf.Block_device,os.O_RDWR,0066)
//	//if err!=nil{
//	//	log.Fatal("OPEN_DEVICE_FAILE")
//	//	return
//	//}
//
//	//设置监听ip和端口
//	lis , err := net.Listen("tcp" , bitstore.TempPort)
//	if err != nil {
//		log.Fatal("Failed to listen: %v" , bitstore.TempPort)
//	}
//	//新建一个server
//	s := grpc.NewServer()
//	//初始化server
//	bitstore.RegisterBitStoreServer(s,&bitstore.BitStoreService{
//		Block_device: block_device,
//	})
//	//启动服务
//
//	if err := s.Serve(lis) ; err != nil {
//		log.Fatalf("failed to serve: %v", err)
//	}
//	log.Println("SUCCESS start service at %v", bitstore.TempPort)
//
//
//}

//func main () {
//	var buffer bytes.Buffer
//	for i:=0 ; i<3 ; i++ {
//		buffer.Write([]byte("abc"))
//	}
//	var temp = make([]byte , buffer.Len() , buffer.Len())
//	fmt.Println(buffer.Read(temp))
//	fmt.Println(temp)
//
//}

func main () {
	var s TTT
	var d TTT
	var err error
	s.data = []byte("abc")
	s.offset = 13320437760
	s.fd , err = syscall.Open("/dev/nvme0n1",os.O_RDWR,0777)
	if err!=nil {
	fmt.Println(err)
	}
	fmt.Println(PUT_test(s))
	d.data = make([]byte, len(s.data), len(s.data))
	d.offset = s.offset
	d.fd = s.fd
	fmt.Println(GET_test(d))

}

type TTT struct {
	fd int
	data	[]byte
	offset 	int64
}

func PUT_test (s TTT)(int,error){
	n,err := syscall.Pwrite(s.fd , s.data , s.offset)
	return n,err

}

func GET_test (s TTT) (int ,[]byte,error){
	n,err := syscall.Pread( s.fd , s.data , s.offset)
	return n,s.data,err
}
