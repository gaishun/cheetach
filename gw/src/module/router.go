package module

import (
	"../config"
	grace "grace"
	"lib/glog"
	"lib/go-humanize"
	"lib/metric"
	"lib/mux"
	"lib/pool"
	"net/http"
	"path/filepath"
	"runtime"
	"../service-direct"
//	"service-direct/model"
	"strconv"
	"log"
//	"strings"
)

type runAllStruct struct {
	region      *string
	ip          *string
	port        *int
	db          *string
	maxCPU      *int
	debug       *bool
	logDir      *string
	tmpDir      *string
	partSize    *string
	maxSize     *string
	imageServer *string
	mds         *string
	unit        *int
}

var startAllDescription = &Module{
	UsageLine: "startAllDescription -port=8080",
	Short:     "start a full function server",
	Long:      `start a full functioon server`,
}

var startAllParam runAllStruct

func init() {
	startAllDescription.Run = runAll
	startAllParam.region = startAllDescription.Flag.String("region", "ng-test", "server service-direct region")
	startAllParam.ip = startAllDescription.Flag.String("ip", "0.0.0.0", "proxy <ip>|<server> address")
	startAllParam.port = startAllDescription.Flag.Int("port", 9086, "http listen port")
	//startAllParam.db = startAllDescription.Flag.String("db", "root:123456@tcp(10.100.10.102:3306)/?allowNativePasswords=true&&charset=utf8", "Namespace DB Addr")
	startAllParam.maxCPU = startAllDescription.Flag.Int("maxCPU", 0, "maximum number of CPUs. 0 means all available CPUs")
	startAllParam.debug = startAllDescription.Flag.Bool("debug", true, "offline debug")
	startAllParam.logDir = startAllDescription.Flag.String("log_dir", "/home/root/ng-proxy/logs", "log dir")
	startAllParam.tmpDir = startAllDescription.Flag.String("tmpdir", "/tmp", "tmp dir")
	startAllParam.partSize = startAllDescription.Flag.String("part_size", "16MiB", "part file size")
	startAllParam.maxSize = startAllDescription.Flag.String("max_size", "5GiB", "max file size")
	startAllParam.imageServer = startAllDescription.Flag.String("image_server", "127.0.0.1:8081", "image server address")
	startAllParam.mds = startAllDescription.Flag.String("mds", "192.168.9.116:4306,192.168.9.115:4307", "mds server address") 
	startAllParam.unit = startAllDescription.Flag.Int("unit",8*1024, "allocate uint")
}

func runAll(md *Module, args []string) bool {
	glog.Infof("%s module", md.Name())

	if *startAllParam.maxCPU < 1 {
		if *startAllParam.debug {
			*startAllParam.maxCPU = runtime.NumCPU()
		} else {
			*startAllParam.maxCPU = int(float64(runtime.NumCPU()) * 0.8)
		}
	}
	runtime.GOMAXPROCS(*startAllParam.maxCPU)
	// init image processor

	if *startAllParam.region == "" {
		glog.Fatal("region must be specify!")
		return false
	}
	config.SetRegion(*startAllParam.region)
	config.Debug = *startAllParam.debug

	var err error
	config.PartSize, err = humanize.ParseBytes(*startAllParam.partSize)
	if err != nil {
		glog.Fatalf("part size set error: %v", err)
	}
	config.MaxSize, err = humanize.ParseBytes(*startAllParam.maxSize)
	if err != nil {
		glog.Fatalf("max size set error: %v", err)
	}

	// read config from db
//	config.Init(func() (*config.Config, error) {
//		return config.ReloadFromDB(*startAllParam.db)
//	})

	// init mds grpc pool
	config.MDSPool = make(map[string]pool.Pool)
	//config.MDSs = strings.Split(*startAllParam.mds, ",")
	for _, mds := range config.MDSs {
		p, err := config.InitGrpcPool(mds)
		if err != nil {
			glog.Fatalf("failed to init mds grpc pool: %v", err)
		}
		config.MDSPool[mds] = p
	}

	// init dss grpc pool
	config.DSSPool = make(map[string]pool.Pool)
	for _, ip := range config.DiskAddrs {
		//for i := 0; i < 4; i++ {
			// addr := fmt.Sprintf("%s:%d", ip, config.BasePort+i)
			//addr := ip + ":" + strconv.Itoa(config.BasePort+i)
			addr := ip
			p, err := config.InitGrpcPool(addr)
			if err != nil {
				glog.Fatalf("failed to init dss grpc pool: %v", err)
			}
			config.DSSPool[addr] = p
		//}
	}

	//集群环境给配置好
	config.SetMasterMds()
	config.SetMasterDss()
	config.SetRepDss()
	//config.SetWorkLoad()
	config.SetMMds2MDss()
	log.Printf("assign\n")
	config.GWassigndss()


	// init odin log
	config.SetLogDir(*startAllParam.logDir)
	config.SetTmpDir(*startAllParam.tmpDir)
	reporter := metric.NewLogReporter(filepath.Join(config.LogDir(), "service_metric.log"), false)
	metric.InitMetric("ResourceFullServer", reporter)

	go config.LogClearDaemon([]config.ClearDir{
		config.ClearDir{Dir: config.LogDir(), ExpireTime: 86400 * 7},
		config.ClearDir{Dir: config.TmpDir(), ExpireTime: 3600},
	})

	if err := config.InitPixar(*startAllParam.imageServer); err != nil {
		glog.Fatalf("failed to init pixar: %v", err)
	}
	config.Mds = *startAllParam.mds
	config.AllocateUint = *startAllParam.unit

	// configure server route
	allRouterMux := mux.NewRouter()

	fromBucket := allRouterMux.PathPrefix("/{bucket}").Subrouter()
	fromBucket.Methods("GET").Path("/{object:.+}").HandlerFunc(service_direct.DownloadHandler)
	fromBucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(service_direct.UploadHandler)
	fromBucket.Methods("DELETE").Path("/{object:.+}").HandlerFunc(service_direct.QueryHandler)

	// start server
	listeningAddress := *startAllParam.ip + ":" + strconv.Itoa(*startAllParam.port)

	if config.Debug {
		if e := http.ListenAndServe(listeningAddress, allRouterMux); e != nil {
			glog.Fatalf("server fail to serve: %v", e)
			return false
		}
	} else {
		if e := grace.ListenAndServe(listeningAddress, allRouterMux); e != nil {
			glog.Fatalf("server fail to serve: %v", e)
			return false
		}
	}

	return true
}
