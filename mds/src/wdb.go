package main

import (   
    "fmt"
	"github.com/tecbot/gorocksdb"
	"strconv"
)

func main() {
	
	// 使用 gorocksdb 连接 RocksDB
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	// 设置输入目标数据库文件（可自行配置，./db 为当前测试文件的目录下的 db 文件夹）
	db, _ := gorocksdb.OpenDb(opts, "./db")
	
	// 创建输入输出流
	ro := gorocksdb.NewDefaultReadOptions()
	//wo := gorocksdb.NewDefaultWriteOptions()
	// 将键为 foo 值为 bar 的键值对写入文件中
	for i := 1;i<=492000;i++{
	var c int=i
		wo := gorocksdb.NewDefaultWriteOptions()
		kkkk := strconv.Itoa(c)
		vvvv := strconv.Itoa(2*c)
		_ = db.Put(wo, []byte(kkkk), []byte(vvvv))
		fmt.Println("Put "+kkkk+"-"+vvvv)
	// 获取数据库中键为 foo 的值
	}
	value, _ := db.Get(ro, []byte("2"))
	defer value.Free()
	// 打印数据库中键为 foo 的值
	fmt.Println("key : 2")
    	fmt.Println("value: ", string(value.Data()[:]))
   	// 删除数据库中键为 foo 对应的键值对
	//_ = db.Delete(wo, []byte("foo"))
}
