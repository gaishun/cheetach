package config_test
//
//import (
//	"../config"
//	"../pkg/util"
//	"testing"
//)
//
//func common(region string) map[int]config.FileStorageConf {
//	fileStorage := map[int]config.FileStorageConf{
//		6:     config.FileStorageConf{ShardId: 6, Region: region, Addr: ""},
//		60000: config.FileStorageConf{ShardId: 60000, Region: region, Addr: ""},
//		60001: config.FileStorageConf{ShardId: 60001, Region: region, Addr: ""},
//		60002: config.FileStorageConf{ShardId: 60002, Region: region, Addr: ""},
//	}
//	return fileStorage
//}
//
//func TestReloadFs(t *testing.T) {
//	writableFs := util.NewSlice()
//	writableFs.Set(1)
//	writableFs.Set(2)
//	t.Error(writableFs)
//
//	fileStorage := common("hxylocal")
//	newWritableSlices := []int{}
//	for _, val := range fileStorage {
//		newWritableSlices = append(newWritableSlices, val.ShardId)
//	}
//
//	writableFs.Replace(newWritableSlices)
//	t.Error(writableFs)
//}
//
//func TestOtherShardFs(t *testing.T) {
//	config.WritableFs = util.NewSlice()
//	fileStorage := common("tianjin")
//	newWritableSlices := []int{}
//	for _, val := range fileStorage {
//		newWritableSlices = append(newWritableSlices, val.ShardId)
//	}
//
//	config.WritableFs.Replace(newWritableSlices)
//	t.Error(config.WritableFs)
//
//	fsshard1 := config.ShardFs("tianjin", "ns", "key")
//	fsshard2 := config.ShardFs("tianjin", "ns", "key2")
//	fsshard3 := config.ShardFs("tianjin", "ns", "key3")
//	fsshard4 := config.ShardFs("tianjin", "ns", "key4")
//	t.Error(fsshard1, fsshard2, fsshard3, fsshard4)
//}
//
//func TestLocalShardFs(t *testing.T) {
//	config.WritableFs = util.NewSlice()
//	fileStorage := common("hxylocal")
//	newWritableSlices := []int{}
//	for _, val := range fileStorage {
//		newWritableSlices = append(newWritableSlices, val.ShardId)
//	}
//	config.WritableFs.Replace(newWritableSlices)
//	t.Error(config.WritableFs)
//
//	fsshard := make([]interface{}, 11)
//
//	fsshard[0] = config.ShardFs("hxylocal", "erplocal", "key")
//	fsshard[1] = config.ShardFs("hxylocal", "hxylocal", "key1")
//	fsshard[2] = config.ShardFs("hxylocal", "leanlocal", "key2")
//	fsshard[3] = config.ShardFs("hxylocal", "phonevoice", "key3")
//	fsshard[4] = config.ShardFs("hxylocal", "phonevoice", "key4")
//	fsshard[5] = config.ShardFs("hxylocal", "phonevoice", "key5")
//	fsshard[6] = config.ShardFs("hxylocal", "phonevoice", "key6")
//	fsshard[7] = config.ShardFs("hxylocal", "phonevoice", "key7")
//	fsshard[8] = config.ShardFs("hxylocal", "phonevoice", "key8")
//	fsshard[9] = config.ShardFs("hxylocal", "phonevoice", "key9")
//	fsshard[10] = config.ShardFs("hxylocal", "phonevoice", "key10")
//	t.Error(fsshard...)
//}
