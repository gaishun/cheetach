package bitstore

import (
	"log"
	"syscall"
)

//var fd, _ = syscall.Open("",os.O_RDWR,0066)
//var str []byte
//var offset int64
//
//var b , _ = syscall.Pwrite(fd , str , offset)
//var suss = syscall.Fsync(fd)

type libaio struct {
	num_pending int
	num_running int
	rval 		int
	length 		int
}

func DWrite (block_device *Block_device_t, offset int64  ,  data []byte) (int,error) {
	n , err := syscall.Pwrite(block_device.Fd_direct , data , offset)
	if err != nil {
		log.Fatal("PWrite ERROR\n")
		return 0,err
	}
	return n,err
}

func  DRead (block_device *Block_device_t, offset int64 , data []byte) (int,error) {
	n,err := syscall.Pread( block_device.Fd_direct , data , offset)
	if err != nil{
		log.Fatal("PRead ERROR\n")
		return 0 , err
	}
	return n,err
}

func  DFsync (fd int) error {

	err := syscall.Fsync(fd)
	if err != nil{
		log.Fatal("Fsync ERROR\n")
		return err
	}
	return nil
}