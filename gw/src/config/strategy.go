package config

import (
	"math/rand"
)

const (
	LocalOtherShardId    = 60000
	LocalOldShardId      = 6
	LocalOldShardId60001 = 60001
	LocalOldShardId60002 = 60002
	LocalOldShardId60003 = 60003
	LocalOldShardId60004 = 60004
	YSInternal           = 103
)

func ShardFs(region, ns, key string) (shard int) {
	switch region {
	case "hxylocal":
		shard = localStrategy(region, ns, key)
	default:
		shard = defaultShardFs(region, ns, key)
	}
	return shard
}

func localStrategy(region, ns, key string) int {
	if ns != "phonevoice" {
		return YSInternal
	}

	// phonevoice
	return defaultShardFs(region, ns, key,
		LocalOldShardId, LocalOtherShardId,
		LocalOldShardId60001, LocalOldShardId60002,
		LocalOldShardId60003, LocalOldShardId60004)
}

/*
 * name :			defaultShardFs
 * Description :	根据ns+key hash选择 gift-fs 防止对0求余
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-09-06
 * correcter :		huangchunhua
 * correct date :	2018-06-11
 * reason :
 * version :	    2.2.0.1
 */
func defaultShardFs(region, ns, key string, filters ...int) int {
	length := len(configData.WriteFSList[region])
	if length == 0 {
		return -1
	}

//	key = fmt.Sprintf("%s#%s", ns, key)
//	hashValue := crc32.ChecksumIEEE([]byte(key))
	pos := rand.Intn(length)
	fsshard := configData.WriteFSList[region][pos]

	// filter local shardId
	counter := 0
	for exist(fsshard, filters) {
		counter++
		pos = (pos + counter) % length
		fsshard = configData.WriteFSList[region][pos]
	}

	return fsshard
}

func exist(fsshard int, filters []int) bool {
	for _, filter := range filters {
		if filter == fsshard {
			return true
		}
	}
	return false
}
