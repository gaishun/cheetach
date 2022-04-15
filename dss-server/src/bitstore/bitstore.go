package bitstore

import (
	"bytes"
	"io"
	"log"
	"syscall"
	"time"
)

type pdata struct {
	length 	uint64
	offset 	uint64
	data	[]byte
}

var (
	TempPort = "127.0.0.1:8081"
	Logdir = "./log"
)

type BitStoreService struct {
	//	UnimplementedBitStoreServer
	Block_device *Block_device_t
}

func (s *BitStoreService) Put(put BitStore_PutServer) error {
	log.Println("revoke PUT operations")
	//var n int
	var err error
	var req *PutRequest
	var resp = new(PutResponse)
	//var wdata []pdata
	//初始化一个容量，避免后期内存复制。
	//wdata = make([]pdata,1000,1000)

	start := time.Now()
	for i:=0 ;true; i++ {
		//question : 怎么确定读取结束。
		req, err = put.Recv()
		if err == io.EOF {
			//log.Println("EOF")
			break
		}
		if err != nil && req != nil {
			//log.Println("PUT***")
			//log.Println(req)
			//log.Println("Recv() error END")
			break
		}
		//两次函数会改变data切片的地址，当然对put方法没什么区别。
		_,err = syscall.Pwrite(s.Block_device.Fd_direct , req.Data , int64(req.Offset))
		if err != nil {
			//落盘err
			log.Println("fail to down disk")
			return err
		}
	}
	err = DFsync(s.Block_device.Fd_direct)
	if err != nil {
		log.Println("fail to sync")
		//同步失败
		return err
	}
	resp.Errcode = 0
	log.Printf("DSS返回点:%v,",time.Since(start).Nanoseconds())
	err = put.SendAndClose(resp)
	if err != nil {
		log.Println("fail to sendandclose")
		//return erri
		log.Println(err)
	}
	//应当返回grpc-success
	return nil
}


func (s *BitStoreService) Get(get BitStore_GetServer) error {
	//log.Println("revoke GET operations")
	var err error
	var req *GetRequest
	var resp = new(GetResponse2)
	var count = 0
	var rdata []byte
	//先把数据都存下来
	start := time.Now()
	for ;true;count++ {
		//question : 怎么确定读取结束。
		req, err = get.Recv();
		if err == io.EOF {
			//ilog.Println("Recv() normal END")
			// log.Println(err)
			//         return nil
			break
		}
		if err != nil {
			log.Println("GET ERROR %v",err)
			//read err  暂时作为结束标志
			//	return nil
			break
		}
		log.Println("GET")
		log.Println(req.Length)
		log.Println(req.Offset)
		rdata = make([]byte, req.Length, req.Length)
		//两次函数会改变data切片的地址
		_, err := syscall.Pread(s.Block_device.Fd_direct, rdata, int64(req.Offset))
		//_ ,err = DRead(s.Block_device ,int64(req.Offset) , rdata[i].data )
		if err != nil {
			log.Fatal("磁盘读取信息失败")
			log.Println(err)
			return err
		}
		//返回数据
		var buffer bytes.Buffer
		buffer.Write(rdata)
		resp.Data = make([]byte, len(rdata), len(rdata))
		_ ,err = buffer.Read(resp.Data)
		if err != nil {
			log.Fatal("copy数据失败")
			log.Println(err)
			return err
		}
		resp.Errcode = 0
		err = get.Send(resp)
		log.Printf("DSS返回点:%v,",time.Since(start).Nanoseconds())
		if err != nil {
			log.Fatal("发送数据失败")
			log.Println(err)
			return err
		}


	}


	return nil
}


