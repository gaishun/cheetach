//package mdsserver
package main

import (
	//mds "../mdspb"
	"../opera"
	"fmt"
	"os"
	//"log"
	"net"
	mds "../mdspb" // 引入编译生成的包
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"github.com/tecbot/gorocksdb"
	//   "io/ioutil"
	"strconv"
	//biu "../biu"
	"math/rand"
	"sync"
	"time"
	"log"
	//"strings"
	"unsafe"
)

const (
	// Address gRPC服务地址
	Address = "100.81.128.73:4306"
)
const (
	DB_PATH = "/disk3/db_path"
	PREFIX_METADATA = "BLOBMD"
)
var db *gorocksdb.DB

type fileMetadata struct {
	name    string
	size    uint64
	disk      uint32
	extents []*mds.Extent
}
var req_double_duan bool
var req_duan_num int
var OFFSET  uint64  //disk_count=4
var hLock []sync.Mutex //全局同步锁
var disk_COUNT int
var max_load []uint64	//通过gw传过来的信息初始化 记录有多少个段
var freeListManager [][][]byte	//通过disk_COUNT确定有多少个主dss与该mds对应。然后通过max_load确定最多有多少段。
var curdisk int
func init_fm () {
	opera.Init_segment(disk_COUNT)
	hLock = make([]sync.Mutex , disk_COUNT , disk_COUNT)
}


func select_dss() uint32 {
	log.Println("select dss~~~\n")
	curdisk++;
	curdisk %=disk_COUNT
	log.Printf("choose dss %v",curdisk )
	return uint32(curdisk)
}

func allocate_space(meta *fileMetadata) int32{
	meta.disk = select_dss()
	m := int(meta.disk)
	var err error
	var extent mds.Extent
	hLock[m].Lock()
	//extent.Offset = 524288
	extent.Offset, err = opera.Assign_block(int(meta.disk),meta.size)
	hLock[m].Unlock()
	if err != nil {
		log.Printf("allocate_spaca occur ERR -> %v",err)
	}
	//extent.Offset , duan_num, double_duan = get_offset(m, block_num)//extent.offset
	//hLock[m].Unlock()

	extent.Length = meta.size//extent.length
	meta.extents = append(meta.extents, &extent)
	//fmt.Println( "meta offset =", extent.Offset)
	//fmt.Println( "meta disk =", meta.disk)
	return 0
}
func save_metadata(meta *fileMetadata) int32{//根据meta信息
	key := PREFIX_METADATA
	key = key + "0"
	key = key + meta.name

	var value string
	//size 16 disk 16 offset 16 length 16 放进去value
	{
		str_size := strconv.Itoa(int(meta.size))
		str_disk := strconv.Itoa(int(meta.disk))
		str_offset :=  strconv.Itoa(int(meta.extents[0].Offset))
		str_length := strconv.Itoa(int(meta.extents[0].Length))
		value = value_encode(str_size) + value_encode(str_disk) +  value_encode(str_offset)+ value_encode(str_length)
	}

	wo := gorocksdb.NewDefaultWriteOptions()
	err := db.Put(wo, []byte(key), []byte(value))
	if err !=nil {
		log.Printf("PUT ERROR ---> %v",err)
	}
	return 0
}
func get_metadata(meta *fileMetadata) int32{//根据meta.name从rocksdb中读取数据，然后按字节拆开，得到object信息，赋值给meta
	key := PREFIX_METADATA
	key = key + "0"
	key = key + meta.name

	ro := gorocksdb.NewDefaultReadOptions()
	value, _ := db.Get(ro, []byte(key))
	defer value.Free()
	str_value := value.Data()[:]
	//fmt.Println("get metadata value: ", string(value.Data()[:]))
	if len(str_value) > 0{
		b_str := str2bytes(string(str_value))

		b_size := b_str[0:16]
		b_disk := b_str[16:32]
		b_offset := b_str[32:48]
		b_length := b_str[48:64]

		str_size := bytes2str(b_size)
		str_disk := bytes2str(b_disk)
		str_offset := bytes2str(b_offset)
		str_length := bytes2str(b_length)

		size,_ := strconv.Atoi(str_size)
		disk,_ := strconv.Atoi(str_disk)
		offset,_ := strconv.Atoi(str_offset)
		length,_ := strconv.Atoi(str_length)
		meta.size = uint64(size)
		meta.disk = uint32(disk)
		var extent mds.Extent
		extent.Offset = uint64(offset)
		extent.Length = uint64(length)
		meta.extents = append(meta.extents, &extent)
	}else {
		fmt.Println("get metadata error")
	}

	return 0
}
func remove_metadata(meta *fileMetadata) int32{

	key := PREFIX_METADATA
	key = key + "0"
	key = key + meta.name

	ro := gorocksdb.NewDefaultReadOptions()
	value, _ := db.Get(ro, []byte(key))
	wo := gorocksdb.NewDefaultWriteOptions()
	errr := db.Delete(wo, []byte(key))
	if errr != nil {
		log.Printf("Object  %v is not Exist,err-->%v",key,errr)
		return 0
	}else {
		log.Printf("delete %v success",key)
	}
	//value, _ := db.Get(ro, []byte(key))
	defer value.Free()
	str_value := value.Data()[:]
	if len(str_value) >0 {
		b_str := str2bytes(string(str_value))
		b_size := b_str[0:16]
		b_disk := b_str[16:32]
		b_offset := b_str[32:48]
		str_size := bytes2str(b_size)
		str_disk := bytes2str(b_disk)
		str_offset := bytes2str(b_offset)
		size,_ := strconv.Atoi(str_size)
		disk,_ := strconv.Atoi(str_disk)
		offset,_ := strconv.Atoi(str_offset)

		opera.Del_block(disk, uint64(offset), uint64(size))
	}else {
		//log.Printf("Object  %v is not Exist",key)
		return 0
	}
	//wo := gorocksdb.NewDefaultWriteOptions()
	//_ = db.Delete(wo, []byte(key))
	return 0
}

// 定义helloService并实现约定的接口
type metadataService struct{
	savedSpaceRequest []*mds.SpaceRequest
}

func (s *metadataService) GetSpace(spaceRequest *mds.SpaceRequest, stream mds.MetadataServer_GetSpaceServer,) error{
	if spaceRequest.Mode == true{//作为副本，已经得到了offset，size，double_duan，duan_num，
		//更新位图
		var meta fileMetadata
		meta.disk = spaceRequest.Ds
		meta.size = spaceRequest.Size
		meta.name = spaceRequest.Name
		var extent = mds.Extent{
			Offset: uint64(spaceRequest.Offset),
			Length: meta.size,
		}
		meta.extents = append(meta.extents , &extent)
		save_metadata(&meta)
		resp := new(mds.SpaceResponse)
		resp.Ret  = 0
		_ = stream.Send(resp)
		return  nil
	}
	
//else 作为主节点
        var meta fileMetadata
        meta.name = spaceRequest.Name
        meta.size = spaceRequest.Size

	start := time.Now()
        var ret int32
        ret = allocate_space(&meta)//四个循环确定分配到哪个block，在meta里面修改，然后成功返回0
        var response mds.SpaceResponse
        response.Ret = ret
        if ret != 0 {
                //fmt.Println( "If ret's count: %d",response.Count)
                if err := stream.Send(&response); err != nil{
                        return err
                }
                return nil
        }
	response.Ds = meta.disk
	response.Count = uint64(len(meta.extents))//count永远等于一
	//for _, extent  := range meta.extents{//循环永远只执行一次。
	response.Extents = append(response.Extents, meta.extents[0])
	//}
	//fmt.Println("Should be first send's count: %d", response.Count)
	_ = stream.Send(&response)
	fmt.Printf("MDSPUT1:%vns.\n",time.Since(start).Nanoseconds())
	//_ = stream.Send(&response)
	save_metadata(&meta)
	//log.Printf("disk %v offset is %v\n",meta.disk,meta.extents[0].Offset)
	//err := opera.Write(meta.name,meta.size,meta.disk, int64(meta.extents[0].Offset),false, int32(0),true)
	//if(err != nil ){
	//	log.Printf("************写副本错误-》，%v\n",err)
	//}
	_ = stream.Send(&response)
	fmt.Printf("MDSPUT2:%vns.\n",time.Since(start).Nanoseconds())
	//_ = stream.Send(&response)
	//workload[meta.disk]+=meta.size//维护每个磁盘的负载
	//workload_add(meta.size , int(meta.disk))

	return nil
}

func (s *metadataService) SaveMetadata(ctx context.Context, MetadataRequest *mds.MetadataRequest) (*mds.MetadataResponse, error){
	metadataresp := new(mds.MetadataResponse)
	//空
	return metadataresp, nil
}

func (s *metadataService) GetResponse(ctx context.Context, ResponseRequest *mds.ResponseRequest) (*mds.MetadataResponse, error){
	metadataresp := new(mds.MetadataResponse)
	metadataresp.Ret = 0;
	return metadataresp, nil
}


func init_rocksdb (){
	rand.Seed(time.Now().Unix())
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 20))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	db, _ = gorocksdb.OpenDb(opts, DB_PATH)
	init_fm()
	log.Println("init finished ")

	var meta fileMetadata
	meta.name = "test"
	meta.size = 8192
	allocate_space(&meta)
	save_metadata(&meta)

	var getmeta fileMetadata
	getmeta.name = "test"
	get_metadata(&getmeta)
	fmt.Println("getmeta: ", getmeta.name,getmeta.size,getmeta.disk)

	var meta2 fileMetadata
	meta2.name = "test"
	remove_metadata(&meta2)

	var meta3 fileMetadata
	meta3.name = "test"
	meta3.size = 8192
	allocate_space(&meta3)
	save_metadata(&meta3)

	log.Println("SUCCESS Start ROCKDB & bitmap\n")

}

//这里是gw和mds建立连接后调用，让mds初始化disk的bitmap和rocksdb的。
func (s *metadataService) AssignDss (ctx context.Context, req *mds.AssignDssRequest) (*mds.AssignDssReponse, error) {
	//db = nil
	if db != nil {
		db.Close()
		db = nil
	}
	assignresp := new(mds.AssignDssReponse)
	//log.Printf("assign dss start, %v \n, %v", req.Duan, req.Count)
	disk_COUNT = int(req.Count)
	//log.Println("assign dss end")

	init_rocksdb()

	if req.Mode == false {
		err:=opera.Assign(nil,req.Count,true)
		if(err != nil ){
			log.Printf("************副本初始化dss个数错误-》")
		}
	}
	assignresp.Ret = 0
	return assignresp, nil
}


func (s *metadataService) RemoveMetadata(ctx context.Context, SpaceRequest *mds.SpaceRequest) (*mds.MetadataResponse, error){
	start := time.Now()
	metadataresp := new(mds.MetadataResponse)
	var meta fileMetadata
	meta.name = SpaceRequest.Name
	meta.size = SpaceRequest.Size
	remove_metadata(&meta)
	metadataresp.Ret = 0
	if SpaceRequest.Mode == false {
		err:=opera.Delete(meta.name,meta.size,true)
		if(err != nil ){
			log.Printf("************副本删除错误-》，%v\n",err)
		}
	}
	fmt.Printf("MDSDEL:%vns.\n",time.Since(start).Nanoseconds())
	return metadataresp, nil
}

func (s *metadataService) GetMetadata(ctx context.Context, SpaceRequest *mds.SpaceRequest) (*mds.MetadataRequest, error){
	start := time.Now()
	metadatareq := new(mds.MetadataRequest)
	var meta fileMetadata
	meta.name = SpaceRequest.Name
	get_metadata(&meta)
	metadatareq.Name = meta.name
	metadatareq.Size = meta.size
	metadatareq.Ds = meta.disk
	metadatareq.Count = uint64(len(meta.extents))
	for _, extent  := range meta.extents{
		metadatareq.Extents = append(metadatareq.Extents, extent)
	}
	fmt.Printf("MDSGET:%vns.\n",time.Since(start).Nanoseconds())
	return metadatareq, nil
}

func str2bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

func bytes2str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
func value_encode (str string) string{
	var b [16]byte
	length := len(str)
	b_str := str2bytes(str)
	//fmt.Println("b_str= ",b_str)
	if length != 16{
		for i :=0; i< 16-length ;i++ {
			b[i] = '0'
		}
		for j := 16-length; j<16;j++ {
			b[j] = b_str[j -(16-length)]
		}
	}
	//fmt.Println("b= ",b)
	var b_str2 string = string(b[:])
	//fmt.Println("b_str2= ",b_str2)
	return b_str2
}
func key_decode (str string) (int,int) {  //提取disk号和段号，‘bb’ + 16位的disk号 + 16位的段号
	b_str := str2bytes(str)
	var b_disk [16]byte
	var b_duan [16]byte

	for i:= 0;i<16;i++{
		b_disk[i] = b_str[2+i]
		b_duan[i] = b_str[18+i]
	}
	var b_disk2 string = string(b_disk[:])
	var b_duan2 string = string(b_duan[:])
	disk, _ := strconv.Atoi(b_disk2)
	duan, _ := strconv.Atoi(b_duan2)
	return disk, duan
}


func main() {
	if len(os.Args) ==2 {
		disk_COUNT, _ = strconv.Atoi(os.Args[1])
		init_rocksdb()
	}

	listen, err := net.Listen("tcp", Address)
	if err != nil {
		grpclog.Fatalf("failed to listen: %v", err)
	}
	// 实例化grpc Server
	s := grpc.NewServer()
	// 注册metadataService
	mds.RegisterMetadataServerServer(s, &metadataService{})
	grpclog.Println("Listen on " + Address)
	opera.Init_pool()

	log.Printf("SUCCESS_START on %v\n",Address)

	s.Serve(listen)
}


