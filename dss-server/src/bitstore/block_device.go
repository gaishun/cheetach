package bitstore


import (
	"../excp"
	"os"
	"syscall"
)

type Block_device_t struct {
	Aio 		bool
	Fd_direct 	int
	Size 		uint64
	Aio_stop 	bool
	Devname 	[]byte
	//Thread_id
}

func Block_device_open (block_device *Block_device_t, path []byte) int {
	var ret  = excp.BITSTORE_OK

	block_device.Aio = true
	ret = Block_device_aio_open(block_device , path)

	return ret
}

func Block_device_aio_open(block_device *Block_device_t, path []byte) int{
	var ret = excp.BITSTORE_OK
	var err error
	block_device.Fd_direct, err = syscall.Open(string(path), os.O_RDWR ,0666)
	if err != nil {
		ret = excp.BITSTORE_ERR_FILE_OPEN
		return ret
	}


	//省略了检查打开的文件的类型的操作，默认打开的文件的信息没有问题
	if false {
		return excp.BITSTORE_ERR_FILE_STAT
	}
	//省略了检查是否是块设备，默认是块设备
	if true {
		var s int64
		ret = get_block_device_size(block_device.Fd_direct , &s)
		if ret != excp.BITSTORE_OK{
			return ret
		}
		block_device.Size = uint64(s)
	}else {
		block_device.Size = 0 //本来写的是 fstat.st_size
	}

	ret = block_device_aio_start(block_device)

	return excp.BITSTORE_OK
}

func block_device_aio_start (block_device *Block_device_t) int {
	var ret = 0
	block_device.Aio_stop = false
	return ret
}

func get_block_device_size  (fd int , psize *int64) int {
	*psize = 7*1024* 1024*1024*1024
	return excp.BITSTORE_OK
}

//func block_device_aio_write (block_device *Block_device_t , )