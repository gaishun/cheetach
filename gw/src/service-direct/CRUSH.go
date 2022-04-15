package service_direct

import (
//	"math/rand"
	"hash/crc32"
//	"log"
	"../config"
)

var (
	pg_num = uint32(1) 	//这里暂定两个pg，一个pg对应一个mds
	mds_num = len(config.MDSs)

	pg_list []uint32
	pg_sum []uint32

	//暂时先不指定mds*对应的mds服务是哪个，就先按config.Mastermds里面的序号来指定。 1-1 2-2
	mds_weight []float64

	size []float64

)

//init
func init (){//暂时先不指定每个pg对应的mds是哪个，就先按config.Mastermds里面的序号来指定。
	pg_list = make([]uint32 , pg_num , pg_num)
	pg_sum = make([]uint32 , pg_num , pg_num)
	size = make([]float64 , mds_num , mds_num)
	mds_weight = make([]float64 , mds_num ,mds_num)
	mds_weight[0] = 0.9
	//mds_weight[1] = 0.9
	pg_list[0]=1
	//pg_list[1]=1
	pg_sum[0] = uint32(0)
	for i:=uint32(1) ;i<pg_num ;i++ {
		pg_sum[i] = pg_sum[i-1] + pg_list[i]
	}
//	log.Printf("init CRUSH information success --> %v %v",pg_list, pg_sum)
}

func Crush_hash_pg_id(pool int, name string) uint32 {

	hashValue := crc32.ChecksumIEEE([]byte(name)) + uint32(pool)
//	log.Printf("object-name = %v choose %v\n" , name,hashValue%pg_num)
	return hashValue%pg_num

}

func Repeat_Repeat_choose (pgid uint32) string {

	maxn := -1.0
	//index := 0
	for osd_id:=0 ; osd_id < int(pg_list[pgid]) ;osd_id++ {
		size[osd_id] = float64(pgid << 5 >>4 <<2 ) + float64((osd_id+7)>>2 <<3 >>4 <<6 >>2)
		if size[osd_id] < 0 {
			size[osd_id] *= -1
		}
		size[osd_id] *= mds_weight[osd_id]
		if maxn < size[osd_id] {
			maxn = size[osd_id]
			//index = osd_id
		}
	}
	return config.MDSs[0]
	//return config.MDSs[pg_sum[pgid]+uint32(index)]
}
