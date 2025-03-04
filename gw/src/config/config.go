package config

import (
	"database/sql"
	"fmt"
	"lib/glog"
	yaml "lib/goyaml"
	_ "lib/mysql"
	pixar "lib/pixar/sdk"
	"lib/pool"
	"lib/weedo"
	"golang.org/x/net/context"
	"os"
	"pkg/util"
	"../service-direct/model"
	//"strconv"
	"strings"
	"sync"
	"log"
	mds "../service-direct/mdspb"
)

var (
	LOGDIR = "/home/gift-proxy/log"
	TMPDIR = "/home/gift-proxy/tmp"
)

type NSConf struct {
	Visibility      string
	Secretkey       string
	Tokenexpire     int64
	Accept          []string
	Spec            map[string]string
	Region          string
	MaxAge          int64
	MirrorAddr      string
	Domain          string
	Owner           string
	Email           string
	Desc            string
	FileNum         int64
	UsedSpace       int64
	RSA             bool
	MaxQps          int64
	WhiteList       map[string]interface{}
	WhiteListEnable bool
	LifeCycle       string
	SegmentEnable   bool
}

type FileStorageAddr struct {
	MasterAddr string
	Status     int32
}

type FileStorageConf struct {
	ShardId  int
	Region   string
	Addr     map[int]*FileStorageAddr //master_id : addr_and_status
	Readonly int
}

type DBConf struct {
	Addr    string
	Maxconn int
	MaxIdle int
}

type LogConf struct {
	LogDir       string
	Level        string
	EnableMetric bool
}

type RsaKey struct {
	PublicKey  string
	PrivateKey string
}

type Config struct {
	Namespace      map[string]NSConf
	FileStorage    map[int]FileStorageConf
	DB             map[string]DBConf
	TmpDir         string
	Log            LogConf
	MappingService map[string]string
	MaxFileSize    int64
	DefaultDomain  map[string]map[string]string //region:visibility:domain
	RsaKeys        map[string]RsaKey
	WriteFSList    map[string][]int
}

// common variabal
var (
	region        string
	ObjectPartMap []model.ObjectPartMapper
	ResourceMap   []model.ResourceMapper
	ImgCacheMap   model.ResourceMapper
	QpsLimit      *util.QpsLimit
	Debug         bool
	PartSize      uint64
	MaxSize       uint64
	Mds           string
	MDSs          =[]string{"100.81.128.73:4306"}
//	MDSs          =[]string{"192.168.9.106:4306"}
)

// disk map addr
//var DiskAddrs = []string{"10.107.19.34"}
//var DiskAddrs = []string{"10.107.9.70:8081","10.107.9.70:8082","10.107.9.70:8083","10.107.9.70:8084"}
/*var DiskAddrs = []string{"192.168.9.105:8081","192.168.9.106:8083","192.168.9.106:8084",
			 "192.168.9.105:8082","192.168.9.107:8083","192.168.9.107:8084",
			 "192.168.9.106:8081","192.168.9.105:8083","192.168.9.107:8084",
			 "192.168.9.106:8082","192.168.9.107:8081","192.168.9.107:8082",}
*/
 //var DiskAddrs = []string{"192.168.9.104:8081",
 //                         "192.168.9.105:8081",
 //                         "192.168.9.106:8081",
 //                         "192.168.9.107:8081",}
var DiskAddrs = []string{"100.69.96.33:8084","11.158.232.168:8084","11.158.242.44:8084"}

//var DiskAddrs = []string{"10.0.9.72"}
var NumMasterDss = 3 //用来记录主dss的数量
var NumMasterMds = 1 //用来记录主mds的数量
var DssReplicateFactor = 0 //用来记录dss的复制因子
//var MdsReplicateFactor = 0 //用来记录mds的复制因子
						//复制因子为1时，0，2，4，6...为主，1，3，5...为副本
						//复制因子为2时，0，3，6...为主节点，12为0的副本节点，45为3的副本节点，类推。
var MasterDss []string
var ReplicationDss map[string][]string
var MasterMds []string 		//slave mds 不用我们来设置，MDS自己管好自己的。
var Mastermds2MasterDss map[string] []string
//var WorkLoadDss map[string]int
// var InitialWorkLoad = []string

func SetMasterMds () {//设置主MDS节点
	MasterMds = MDSs
	log.Printf("master MDS is %v",MasterMds)

}

func SetMasterDss () {//设置主DSS节点
	var j = 0
	MasterDss = make([]string, NumMasterDss , NumMasterDss)
	for i := 0; j < NumMasterDss; i= i+1+DssReplicateFactor {
		MasterDss[j] = DiskAddrs[i]
		j++
	}
	log.Printf("master DSS is %v",MasterDss)
}

func SetRepDss () {	//设置每个主DSS节点对应的副本节点
	ReplicationDss = make(map[string][]string)
	for i := 0; i < NumMasterDss; i++ {
		ReplicationDss[MasterDss[i]] = make([]string , DssReplicateFactor , DssReplicateFactor)
		for k:=1 ; k<=DssReplicateFactor ;k++ {
			ReplicationDss[MasterDss[i]][k-1] = DiskAddrs[i*DssReplicateFactor+i+k]
		}
		log.Printf("dss:%v's replication dss is %v",MasterDss[i],ReplicationDss[MasterDss[i]] )
	}
}

func SetMMds2MDss (){ //设置主Mds节点对应的主Dss节点
	Mastermds2MasterDss = make(map[string][]string)
	Mastermds2MasterDss[MasterMds[0]] = append(Mastermds2MasterDss[MasterMds[0]],MasterDss[0])
	Mastermds2MasterDss[MasterMds[0]] = append(Mastermds2MasterDss[MasterMds[0]],MasterDss[1])
	Mastermds2MasterDss[MasterMds[0]] = append(Mastermds2MasterDss[MasterMds[0]],MasterDss[2])

	//Mastermds2MasterDss[MasterMds[0]] = append(Mastermds2MasterDss[MasterMds[0]],MasterDss[1])
	//Mastermds2MasterDss[MasterMds[0]] = append(Mastermds2MasterDss[MasterMds[0]],MasterDss[2])
	//Mastermds2MasterDss[MasterMds[1]] = append(Mastermds2MasterDss[MasterMds[1]],MasterDss[1])
	//Mastermds2MasterDss[MasterMds[1]] = append(Mastermds2MasterDss[MasterMds[1]],MasterDss[3])
	log.Printf("Mds:%v's  dss is %v",MasterMds[0],Mastermds2MasterDss[MasterMds[0]] )
	//log.Printf("Mds:%v's  dss is %v",MasterMds[1],Mastermds2MasterDss[MasterMds[1]] )
}

func GWassigndss (){ //给mds主节点发信息告诉他有多少小弟
	var req mds.AssignDssRequest
	var conn pool.Conn
	var err error
        log.Printf("start ass")

	for i := 0; i < len(MasterMds); i++ {
		req.Count = int32(len(Mastermds2MasterDss[MasterMds[i]]))
		req.Duan = make([]uint64 , req.Count, req.Count )
		log.Printf("Count is %v , Duan is %v",req.Count , req.Duan)
		for j:=int32(0) ; j < req.Count; j++ {
			req.Duan[j] = 2097162 //多少段 ， 每段128块，每块8kb
		}
		//以上初始化好信息。
		conn , err = MDSPool[MasterMds[i]].Get()
		if err != nil {
			log.Fatalf("分配dss建立连接失败， --》 %v\n",err)
		}
		client := mds.NewMetadataServerClient(conn.Value())
		resp , err := client.AssignDss(context.Background() , &req)
		log.Printf("get resp2 information， --》 %v\n",resp)
		if err != nil {
			log.Printf("%v 分配dss发送信息失败， --》 %v\n",MasterMds[i],err)
		}
		if resp.Ret != 0 {
			log.Printf("%v 分配dss发送信息失败， --》   \n",MasterMds[i])
		}
		log.Printf("mds:%v's return message is %v",MasterMds[i],resp.Ret)
	}
	log.Printf("end ass")
	conn.Close()

}

//func SetWorkLoad () {//设置每个Dss节点的Workload
//	WorkLoadDss = make(map[string]int)
//	for i := 0; i < NumMasterDss; i++ {
//		WorkLoadDss[MasterDss[i]] = 0
//		}
//}



						

// const BasePort = 8081
// const Replication = 4

// disk id: [0, 12)
// port: [8001, 8012]
// func GetDiskServer(diskid int) []string { 
// 	addrs := make([]string, Replication)
// 	port := strconv.Itoa(BasePort + diskid)

// 	for i := 0; i < Replication; i++ {
// 		addrs[i] = DiskAddrs[i] + ":" + port
// 	}

// 	return addrs
// }

// common constants
const (
	IMGREGION = "imgcache"
	TRYTIME   = 3
	IMGLOG    = "Request=[%s] requestId=[%s] module=[%s] retrynum=[%d] namespace=[%s] resourceKey=[%s] spec=[%s] status=[%d] errmsg=[%s]"
)

// config varibal
var (
	configData   *Config
	initialized  bool          = true
	rwMutex      *sync.RWMutex = new(sync.RWMutex)
	version      uint64        = 0
	Pixar        *pixar.Pixar
	AllocateUint = 0
	MDSPool      map[string]pool.Pool
	DSSPool      map[string]pool.Pool
)

func InitGrpcPool(address string) (pool.Pool, error) {
	opt := pool.DefaultOptions
	opt.MaxIdle = 64
	opt.MaxActive = 64
	opt.MaxConcurrentStreams = 128
	opt.Reuse = true

log.Printf("aaaaaaaa: %s\n", address)
	return pool.New(address, opt)
}

/* 函数指针 */
type Loader func() (*Config, error)

/*
 * name :			Init
 * Description :	初始化
 * input :
 * output :
 * return :
 * creator :		HuangChunhua
 * creat date :		2017-07-25
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func Init(loader Loader) {
	data, err := loader()
	if err != nil && data != nil {
		panic(err)
	}
	configData = data
	initialized = true
}

func InitPixar(address string) (err error) {
	Pixar, err = pixar.New(address)
	return err
}

/*
 * name :			NewWeedoClient
 * Description :	多库版本的NewWeedoClient
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-09-06
 * correcter :
 * correct date :
 * reason :
 * version :	2.1.0.1
 */
func NewWeedoClient(fsShard int) (fsclis *weedo.Client, err error) {
	val, has := RequireConfigData().FileStorage[fsShard]
	if !has || len(val.Addr) == 0 {
		return nil, fmt.Errorf("no gift-fs the fsShard is: %d", fsShard)
	}

	for i := 0; i < len(val.Addr); i++ {
		if val.Addr[i].Status == 1 {
			fsclis = weedo.NewClient(val.Addr[i].MasterAddr)
			return fsclis, nil
		}
	}

	fsclis = weedo.NewClient(val.Addr[0].MasterAddr)

	return fsclis, nil
}

/*
 * name :			GetFsShard
 * Description :	获取region对应的多个gift-fs
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-09-06
 * correcter :
 * correct date :
 * reason :
 * version :	2.1.0.0
 */
func GetFsShard(region string) []int {
	fsShards := []int{}
	for _, item := range RequireConfigData().FileStorage {
		if item.Region == region {
			fsShards = append(fsShards, item.ShardId)
		}
	}
	return fsShards
}

/*
 * name :			ReloadFromDB
 * Description :	从DB中读取配置
 * input :
 * output :
 * return :
 * creator :		HuangChunhua
 * creat date :		2017-07-25
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func ReloadFromDB(dbAddr string) (*Config, error) {
	data, err := loadFromDB(dbAddr)
	if err != nil {
		glog.Fatalf("error to load db %s, error: %v", dbAddr, err)
		return nil, err
	}
	glog.Infof("reload config successful")

	monitorConfigDB(dbAddr)
	monitorFS()
	return data, nil
}

/*
 * name :			loadFromDB
 * Description :	读取配置
 * input :
 * output :
 * return :
 * creator :		HuangChunhua
 * creat date :		2017-07-25
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func loadFromDB(dbAddr string) (*Config, error) {
	var db *sql.DB
	db, err := sql.Open("mysql", dbAddr)
	if err != nil {
		glog.Errorf("ReloadConfig failure from DB:%s", dbAddr)
		return nil, err
	}
	defer db.Close()
	//data := new(Config)
	var data *Config
	err, updated := DBToMap(db, &data)
	if err != nil {
		glog.Errorf("ReloadConfig failure from db:%s", dbAddr)
		return nil, err
	}
	if updated == 0 {
		data = nil
	} else {
		glog.Info("ReloadConfig OK!")
		d, _ := yaml.Marshal(&data)
		glog.Infof("data : %s\n", d)
	}
	return data, nil
}

/*
 * name :			RequireConfigData
 * Description :	获取配置信息。
 * input :
 * output :
 * return :
 * creator :		HuangChunhua
 * creat date :		2017-07-25
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func RequireConfigData() *Config {
	if !initialized {
		glog.Errorf("ConfigData is null")
		return nil
	}
	rwMutex.RLock()
	defer rwMutex.RUnlock()
	return configData
}

/*
 * name :			GetDBConf
 * Description :	获取mysql配置信息
 * input :
 * output :
 * return :
 * creator :		ShiMingYa
 * creat date :		2017-07-25
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func (c *Config) GetDBConf(region string) *DBConf {
	if c.DB == nil {
		return nil
	}
	dbconf, has := c.DB[region]
	if !has {
		return nil
	}
	return &dbconf
}

/*
 * name :			GetMappingService
 * Description :	获取query模块主机的host
 * input :
 * output :
 * return :
 * creator :		ShiMingYa
 * creat date :		2017-07-25
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func (c *Config) GetMappingService(region string) string {
	if c.MappingService == nil {
		return ""
	}
	host, has := c.MappingService[region]
	if !has {
		return ""
	}
	return host
}

func SetRegion(r string) {
	region = r
}

func GetRegion() string {
	return region
}

func SetLogDir(dir string) {
	LOGDIR = dir
}

func LogDir() string {
	return LOGDIR
}
func SetTmpDir(dir string) {
	TMPDIR = dir
}
func TmpDir() string {
	return TMPDIR
}

/*
 * name :			DBToMap
 * Description :	读取配置
 * input :
 * output :
 * return :
 * creator :		HuangChunhua
 * creat date :		2017-07-25
 * correcter :
 * correct date :
 * reason :
 * version :	2.1.0.1
 */
func DBToMap(db *sql.DB, ppConfig **Config) (error, uint8) {
	var target_version uint64

	if _, err := db.Exec("USE gift_conf"); err != nil {
		glog.Errorf("ReloadingConfig use database config failure %s\n", err.Error())
		return err, 0
	}

	err := db.QueryRow("SELECT version FROM version_control WHERE hostname=\"TARGET\" ").Scan(&target_version)

	if err != nil {
		glog.Errorf("select from version_control failed\n")
		return err, 0
	}

	if target_version <= version {
		glog.Infof("current_version is %d no need to update\n", version)
		return nil, 0
	}

	glog.Infof("need update to version %d->%d\n start updating\n", version, target_version)
	version = target_version
	*ppConfig = new(Config)
	pConfig := *ppConfig

	rows, err := db.Query("SELECT namespace, visibility, secret_key, token_expire, accept, spec, region, maxage, mirror_addr, domain, owner, email, rsa, max_qps, white_list, white_list_enable,lifecycle,segment_enable FROM namespace_config")
	if err != nil {
		glog.Errorf("select from namespace_config failed\n")
		return err, 0
	}
	defer rows.Close()

	pConfig.Namespace = make(map[string]NSConf)
	var namespace, visibility, secret_key, accept, spec, region, mirror_addr, domain, owner, email, white_list, lifecycle string
	var token_expire, max_age int64
	var rsa, white_list_enable, segment_enable bool
	var max_qps int64
	for rows.Next() {
		if err := rows.Scan(&namespace, &visibility, &secret_key, &token_expire,
			&accept, &spec, &region, &max_age, &mirror_addr, &domain, &owner,
			&email, &rsa, &max_qps, &white_list, &white_list_enable, &lifecycle, &segment_enable); err != nil {
			glog.Errorf("scan namespace_config failed\n")
			return err, 0
		}

		/*
		 * 将深圳ns的region强制变为永顺
		 * 机房迁移遗留问题
		 */
		if region == "shenzhen" {
			region = "ys01"
		}

		var spec_data map[string]string
		err := yaml.Unmarshal([]byte(spec), &spec_data)
		if err != nil {
			glog.Errorf("unmarshal spec err: %v \n", err)
		}

		white_list_data := make(map[string]interface{})
		strs := strings.Split(white_list, ",")
		for _, v := range strs {
			white_list_data[v] = nil
		}

		pConfig.Namespace[namespace] = NSConf{
			Visibility:  visibility,
			Secretkey:   secret_key,
			Tokenexpire: token_expire,
			Accept: func(string) []string {
				if accept != "" {
					return strings.Split(accept, ",")
				} else {
					return nil
				}
			}(accept),
			Spec:            spec_data,
			Region:          region,
			MaxAge:          max_age,
			MirrorAddr:      mirror_addr,
			Domain:          domain,
			Owner:           owner,
			Email:           email,
			RSA:             rsa,
			MaxQps:          max_qps,
			WhiteList:       white_list_data,
			WhiteListEnable: white_list_enable,
			LifeCycle:       lifecycle,
			SegmentEnable:   segment_enable,
		}
	}

	rows, err = db.Query("SELECT namespace, public_key, private_key FROM rsa_key")
	if err != nil {
		glog.Errorf("select from rsa_key failed\n")
		return err, 0
	}
	defer rows.Close()

	pConfig.RsaKeys = make(map[string]RsaKey)
	var public_key, private_key string
	for rows.Next() {
		if err := rows.Scan(&namespace, &public_key, &private_key); err != nil {
			glog.Errorf("scan rsa_key failed\n")
			return err, 0
		}

		pConfig.RsaKeys[namespace] = RsaKey{
			PublicKey:  public_key,
			PrivateKey: private_key,
		}
	}

	rows, err = db.Query("SELECT shardid,region,addr,readonly FROM file_storage")
	if err != nil {
		glog.Errorf("select from file_storage failed\n")
		return err, 0
	}
	defer rows.Close()

	var shardid, readonly int
	var addr string
	pConfig.FileStorage = make(map[int]FileStorageConf)
	pConfig.WriteFSList = make(map[string][]int)
	for rows.Next() {
		if err := rows.Scan(&shardid, &region, &addr, &readonly); err != nil {
			glog.Errorf("scan namespace_config failed\n")
			return err, 0
		}

		pConfig.FileStorage[shardid] = FileStorageConf{
			ShardId:  shardid,
			Region:   region,
			Addr:     make(map[int]*FileStorageAddr),
			Readonly: readonly,
		}
		addrs := strings.Split(addr, ",")
		for i := range addrs {
			pConfig.FileStorage[shardid].Addr[i] = &FileStorageAddr{
				MasterAddr: addrs[i],
				Status:     0,
			}
		}

		// 如果非只读库，那么加入到可写子集群中
		if readonly == 0 {
			pConfig.WriteFSList[region] = append(pConfig.WriteFSList[region], shardid)
		}
	}

	rows, err = db.Query("SELECT db_name,addr,max_conn,max_idle FROM db_conf")
	if err != nil {
		glog.Errorf("select from db_conf failed\n")
		return err, 0
	}
	defer rows.Close()
	var max_conn, max_idle int
	var db_name, db_addr string

	pConfig.DB = make(map[string]DBConf)
	for rows.Next() {
		if err := rows.Scan(&db_name, &db_addr, &max_conn, &max_idle); err != nil {
			glog.Errorf("scan db_conf failed\n")
			return err, 0
		}
		pConfig.DB[db_name] = DBConf{
			Addr:    db_addr,
			Maxconn: max_conn,
			MaxIdle: max_idle,
		}
	}

	var path string
	err = db.QueryRow("SELECT path FROM tmp_dir limit 1").Scan(&path)

	if err != nil {
		glog.Errorf("select tmp_dir failed\n")
		return err, 0
	}
	pConfig.TmpDir = path

	var log_dir, level string
	var enable_metric bool
	err = db.QueryRow("SELECT log_dir, level, enable_metric FROM log_conf limit 1").Scan(&log_dir, &level, &enable_metric)

	if err != nil {
		glog.Errorf("select tmp_dir failed\n")
		return err, 0
	}
	pConfig.Log = LogConf{
		LogDir:       log_dir,
		Level:        level,
		EnableMetric: enable_metric,
	}

	rows, err = db.Query("SELECT dest_name,addr FROM mapping_service")
	if err != nil {
		glog.Errorf("select from mapping_service failed\n")
		return err, 0
	}
	defer rows.Close()
	var dest_name, dest_addr string

	pConfig.MappingService = make(map[string]string)
	for rows.Next() {
		if err := rows.Scan(&dest_name, &dest_addr); err != nil {
			glog.Errorf("scan mapping_service failed\n")
			return err, 0
		}
		pConfig.MappingService[dest_name] = dest_addr
	}

	var size int64
	err = db.QueryRow("SELECT size FROM max_file_size limit 1").Scan(&size)

	if err != nil {
		glog.Errorf("select max_file_size failed\n")
		return err, 0
	}
	pConfig.MaxFileSize = size

	rows, err = db.Query("SELECT shard_name,visibility,addr FROM default_domain")
	if err != nil {
		glog.Errorf("select from defaule_domain failed\n")
		return err, 0
	}
	defer rows.Close()
	var shard_name, domain_visibility, domain_addr string

	pConfig.DefaultDomain = make(map[string]map[string]string)
	for rows.Next() {
		if err := rows.Scan(&shard_name, &domain_visibility, &domain_addr); err != nil {
			glog.Errorf("scan db_conf failed\n")
			return err, 0
		}

		_, ok := pConfig.DefaultDomain[shard_name]
		if !ok {
			pConfig.DefaultDomain[shard_name] = make(map[string]string)
		}
		pConfig.DefaultDomain[shard_name][domain_visibility] = domain_addr
	}
	host, _ := os.Hostname()
	_, err = db.Exec("INSERT version_control SET hostname=?,version=? ON DUPLICATE KEY UPDATE version=?", host, version, version)
	if err != nil {
		glog.Errorf("update verion to DB failed")

	}
	glog.Infof("update ended\n")
	return nil, 1
}
