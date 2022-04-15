package main

import (   
    "fmt"
	"github.com/tecbot/gorocksdb"
//	"strconv"
)

func main() {
	
	// 使用 gorocksdb 连接 RocksDB
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	// 设置输入目标数据库文件（可自行配置，./db 为当前测试文件的目录下的 db 文件夹）
	db, _ := gorocksdb.OpenDb(opts, "/home/zym/Desktop/lrm/example/db")
	/*for i := 1;i<=5;i++{
		cstr := strconv.Itoa(i)
		ro := gorocksdb.NewDefaultReadOptions()
		value, _ := db.Get(ro, []byte(cstr))
		defer value.Free()
		// 打印数据库中键为 foo 的值
		fmt.Println("Get "+cstr+"-"+string(value.Data()[:]))
	}*/


	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	it:=db.NewIterator(ro)
	defer it.Close()
	it.Seek([]byte("1"))
	for it =it;it.Valid();it.Next(){
		key:=it.Key()
		value :=it.Value()
		fmt.Printf("%s - %s\n",key.Data(),value.Data())
		key.Free()
		value.Free()
	}
}
