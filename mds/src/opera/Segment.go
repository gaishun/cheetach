package opera

import (
	"errors"
	"fmt"
	//"log"
)

var sectors uint64		//num of alloct_uint
var alloct_uint uint64 	//I/O uints ->bytes
var freeList  [][]bool	//第一维表示有几个VG，第二维表示该维度上每个sectors的状态
var seg_list  [][][]node
//第一维代表盘的数量
//第二维索引代表 0->1kb，1->2kb，2->4kb，3->8kb，4->16kb，5->32kb，6->64kb,7->128kb,8->256kb,9->512kb
//第三位代表忙闲和位偏移
var chain_size = []uint64{1*1024,2*1024,4*1024,8*1024,16*1024,32*1024,64*1024,128*1024,256*1024,512*1024}

const (
	DB_PATH = "/mnt/nvme11"
	ALLOCATE_UNIT = 512 //bytes
	SECTOR_NUM = 2097152000 // 512bytes/sector --> 100G
	PREFIX_METADATA = "BLOBMD"
)

type node struct {
	address		uint64
	busy		bool
}

func get_newBlock (ds int,index int) error {
	var i, j, k int
	var off uint64
	var end bool
	for j = index + 1; j < 10; j++ {
		for i = 0; i < len(seg_list[ds][j]); i++ {
			if seg_list[ds][j][i].busy == false {
				end = true
				off = seg_list[ds][j][i].address                                        //确定偏移量，后面就可以不用了
				seg_list[ds][j] = append(seg_list[ds][j][:i], seg_list[ds][j][i+1:]...) //把这个node从这个块列表中删掉
				break
			}
		}
		if end {
			break
		}
	}
	if end == false{
		return errors.New(fmt.Sprintf("There is No Space for size %v! ! ! ! \n",chain_size[index]))
	}
	/******/
	//第一个块要append两次，例：512=256+128+64+32+16+8+8
	k = index
	seg_list[ds][k] = append(seg_list[ds][k], node{address: off, busy: false})
	off += uint64(chain_size[k])
	for ; k < j; k++ {
		seg_list[ds][k] = append(seg_list[ds][k], node{address: off, busy: false})
		off += uint64(chain_size[k] )
	}
	return nil
	//return errors.New("There is No Space ! ! ! ! \n")
}

func get_offset (ds int ,index int , offset* uint64) error{
	i := 0
	for ; i<len(seg_list[ds][index]); i++ {
		if seg_list[ds][index][i].busy != true {
			seg_list[ds][index][i].busy = true
			*offset = seg_list[ds][index][i].address
			return nil
		}
	}
	err := get_newBlock(ds,index)
	if err != nil {
		return err
	}
	//执行到这里必然给安排上这个块
	seg_list[ds][index][i].busy = true
	*offset = seg_list[ds][index][i].address
	return nil
}

func assign_block (disk_id int ,size uint64,length* uint64,index int,offset* uint64) error {
	var err error
	if index > 9 {//也即是说，最大只分配524288 bytes 大小的块
		return nil
	}
	if size > chain_size[index] {
		return assign_block(disk_id,size,length , index+1,offset)
	}
//	log.Printf("assign size is %v , chain_size is %v",size,chain_size[index])
	*length = size

	err = get_offset(disk_id,index, offset)

	return err
}

func Assign_block(disk_id int, size uint64) (uint64, error) {
	var err error
	var length uint64
	var index = 0
	var offset uint64
	err = nil
	err = assign_block(disk_id,size,&length,index,&offset)
	return  offset,err
}

func insertHighWithSort (ds int,index int,offset uint64){
	seg_list[ds][index] = append(seg_list[ds][index],node{busy: false,address: offset})
	//需要再排一下第index级的顺序
}

//取消空间分配
func Del_block (ds int, offset uint64 , size uint64) {
	var i int
	for i =0 ; i<10 ;i++ {
		if size <= chain_size[i]{
			break
		}
	}

	for j:=0;j< len(seg_list[ds][i]);j++{//因为这个肯定是按大小排好序的，这里可以换用二分法进行查找，速度更快
		if seg_list[ds][i][j].address == offset {
			seg_list[ds][i][j].busy = false
			if i!=9 && j>0 && seg_list[ds][i][j-1].busy == false {
				seg_list[ds][i] = append(seg_list[ds][i][:j-1],seg_list[ds][i][j+1:]...)
				insertHighWithSort(ds,i+1,offset-uint64(chain_size[i]))
			}
			if i!=9 && j<len(seg_list[ds][i])-1 && seg_list[ds][i][j+1].busy == false{
				seg_list[ds][i] = append(seg_list[ds][i][:j],seg_list[ds][i][j+2:]...)
				insertHighWithSort(ds,i+1,offset)
			}
		}
	}

}

func Init_segment (disk_num int) {
	length := SECTOR_NUM/(1024*8)//512*1024*8(512kB) / 512(512bytes)
	seg_list = make([][][]node,disk_num,disk_num)
	for i:=0 ;i<disk_num ;i++{
		seg_list[i] =  make([][]node,10,10)
		seg_list[i][9] = make([]node,length,length)
		for j:=0 ;j<length ;j++{
			seg_list[i][9][j].busy = false
			seg_list[i][9][j].address = uint64(j*512*1024)
		}
	}
}

