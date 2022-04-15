package main

import "syscall"

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
