package service_direct

import (
	"bytes"
	"../config"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"lib/glog"
	"lib/mux"
	"lib/uuid"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"pkg/hash"
	"regexp"
	"runtime"
	"runtime/debug"
	"./model"
	"strconv"
	"strings"
	"time"
	//"log"
)

const (
	VERSION = "2.6.1"
)

type Context struct {
	ns           string
	key          string
	spec         string
	fsShard      int
	fsKey        string
	fsType       string
	maxAge       int64
	size         int64
	host         string
	requestId    string
	mime         string
	filename     string
	md5          string
	createTime   int64
	start        time.Time
	processTime  int64
	req          *http.Request
	result       *Result
	fsPublicUrl  string
	fsUrl        string
	reader       io.Reader
	fsResp       *http.Response
	odin         OdinMetric
	extend       map[string]interface{}
	partId       int
	objectPart   model.ObjectPart
	resource     model.Resource
	weedMimeType string
	mdsStart     time.Time
	mdsTime      int64
	dssStart     time.Time
	dssTime      int64
	mdsChan      chan int
	dssChan      chan error
	offset       uint64
	length       uint64
}

const (
	DefaultUploadID   = ""
	MaxQueryBatchSize = 10
	Characters        = "123456789abcdefghijklmnpqrstuvwxyzABCDEFGHIJKLMNPQRSTUVWXYZ"
	KeySize           = 20
	REQUESTTIMEOUT    = 300
	MagicNum          = "do1_"
	Origin            = "origin"
	LOGTEMPLATE       = "Request=[%s] requestId=[%s] host=[%s] remoteAddr=[%s] method=[%s] url=[%s] namespace=[%s] resourceKey=[%s] fsShard=[%d] fsUrl=[%s] fsKey=[%s] fsType=[%s] status=[%d] process_time=[%d] file_size=[%d] errmsg=[%s]"
)

const (
	UPLOADWITHKEY_CALLER    = "UploadResourceWithKey"
	UPLOADWITHOUTKEY_CALLER = "UploadResourceWithoutKey"
	DOWNLOAD_CALLER         = "StaticResource"
	IMAGE_CALLER            = "ImageProcess"
	QUERY_CALLER            = "QueryResource"
	FSKEY_CALLER            = "QueryFskey"
	BATCHQUERY_CALLER       = "BatchQueryResource"
)

const (
	DFS  = "dfs"
	MDFS = "mdfs"
)

type Result struct {
	ErrorCode *ErrorCode
	ErrorMsg  string
}

type NsLifeCycle struct {
	TTL string `json:"ttl"`
}

var (
	formats = map[string]bool{"bmp": true, "jpg": true, "jpeg": true, "png": true, "tiff": true, "gif": true, "webp": true}
)

/*
 * name :			extractUrl
 * Description :	解析url,获取ns、key、spec
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
func extractUrl(path string, filter string) (ns, key, spec string) {
	if strings.HasPrefix(path, filter) {
		path = strings.TrimPrefix(path, filter)
	}
	regExp := regexp.MustCompile(`^(?P<ns>[^\/]+)\/?(?P<key>[^\!]+)\!?(?P<spec>[a-zA-Z0-9\-_]*).*$`)
	matches := regExp.FindStringSubmatch(path)
	if len(matches) > 0 && matches[0] == path {
		if !strings.Contains(path, "/") {
			return path, "", ""
		}
		if strings.HasSuffix(path, "/") && strings.Count(path, "/") == 1 {
			return path[:len(path)-1], "", ""
		}
		if strings.HasPrefix(matches[2], MagicNum) {
			fileExt := filepath.Ext(matches[2])
			//remove ext in key ,like do1_aaaaaa.jpg trim to do1_aaaaaa
			storedKey := strings.TrimSuffix(matches[2], fileExt)
			return matches[1], storedKey, matches[3]
		}
		return matches[1], matches[2], matches[3]
	}

	return "", "", ""
}

/*
 * name :			Health
 * Description :
 * input :
 * output :
 * return :
 * creator :		huangchunhua
 * creat date :		2017-08-04
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func Health(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("alive"))
}

/*
 * name :			Status
 * Description :
 * input :
 * output :
 * return :
 * creator :		huangchunhua
 * creat date :		2017-08-04
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func Status(w http.ResponseWriter, r *http.Request) {
	stat := make(map[string]interface{})
	stat["cpu"] = runtime.NumCPU()
	stat["goroutine"] = runtime.NumGoroutine()
	stat["cgocall"] = runtime.NumCgoCall()
	gcStat := &debug.GCStats{}
	debug.ReadGCStats(gcStat)
	stat["gc"] = gcStat.NumGC
	stat["pausetotal"] = gcStat.PauseTotal.Nanoseconds()
	stat["version"] = VERSION

	bytes, err := json.Marshal(stat)
	if err != nil {
		bytes = []byte("json marshal error")
	}
	w.Header().Set("Content-Type", "application/json;charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(bytes)
}

/*
 * name :			prepare
 * Description :	解析表单，生成context，打印日志
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-08-03
 * correcter :
 * correct date :
 * reason :
 * version :	2.002
 */
func prepare(req *http.Request, filter string) *Context {
	req.ParseForm()
	context := &Context{
		requestId: uuid.New(),
		start:     time.Now(),
		result:    &Result{Success, ""},
		extend:    make(map[string]interface{}),
		mdsChan:   make(chan int),
		dssChan:   make(chan error),
	}

	context.req = req
	vars := mux.Vars(req)
	context.ns = vars["bucket"]
	context.key = vars["object"]
	//context.ns, context.key, context.spec = extractUrl(req.URL.Path, filter)

	if context.spec == "" {
		context.spec = Origin
	}

	var err error
	context.host, err = os.Hostname()
	if err != nil {
		context.host = "unknown"
	}
	/*glog.Infof(LOGTEMPLATE, "start",
		context.requestId,
		context.host,
		req.RemoteAddr,
		req.Method,
		req.RequestURI,
		context.ns,
		context.key,
		context.fsShard,
		context.fsPublicUrl,
		context.fsKey,
		"",
		context.result.ErrorCode.StatusCode,
		context.processTime,
		context.size,
		context.result.ErrorMsg,
	)*/

	return context
}

/*
 * name :			ErrorResp
 * Description :	返回错误信息
 * input :
 * output :
 * return :
 * creator :		huangchunhua
 * creat date :		2017-08-04
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func ErrorResp(res http.ResponseWriter, ctx *Context) {
	m := make(map[string]interface{})
	m["status"] = "failure"
	m["status_code"] = ctx.result.ErrorCode.StatusCode
	m["errcode"] = ctx.result.ErrorCode.StatusMsg
	m["errmsg"] = ctx.result.ErrorMsg

	bytes, err := json.Marshal(m)
	if err != nil {
		bytes = []byte("json marshal error")
	}
	res.Header().Set("Content-Type", "application/json;charset=UTF-8")
	res.Header().Set("x-request-id", ctx.requestId)
	res.Header().Set("x-gift-server", ctx.host)
	res.WriteHeader(ctx.result.ErrorCode.StatusCode)
	_, err = res.Write(bytes)
	return
}

/*
 * name :			SuccessResp
 * Description :	返回成功信息
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-08-14
 * correcter :
 * correct date :
 * reason :
 * version :	2.003
 */
func replaceSpecialCharacters(src []byte) (ret []byte) {
	ret = bytes.Replace(src, []byte("\\u003c"), []byte("<"), -1)
	ret = bytes.Replace(src, []byte("\\u003e"), []byte(">"), -1)
	ret = bytes.Replace(src, []byte("\\u0026"), []byte("&"), -1)
	return ret
}
func SuccessResp(res http.ResponseWriter, data map[string]interface{}, ctx *Context) {
	data["status"] = "success"
	data["status_code"] = http.StatusOK
	bytes, err := json.Marshal(data)
	if err != nil {
		bytes = []byte("json marshal error")
	}
	res.Header().Set("Content-Type", "application/json;charset=UTF-8")
	res.Header().Set("x-request-id", ctx.requestId)
	res.Header().Set("x-gift-server", ctx.host)
	res.WriteHeader(http.StatusOK)
	_, err = res.Write(replaceSpecialCharacters(bytes))

	return
}

/*
 * name :			Recovery
 * Description :	捕捉panic
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-08-28
 * correcter :
 * correct date :
 * reason :
 * version :	2.003
 */
func Recovery(res http.ResponseWriter, ctx *Context) {
	stackInfo := func() string {
		buf := make([]byte, 1024)
		n := runtime.Stack(buf, false)
		return string(buf[:n])
	}
	if err := recover(); err != nil {
		glog.Errorf("Request=[%s] requestId=[%s] errmsg=[%s]", "panic", ctx.requestId, stackInfo())
		ctx.result = &Result{InternalError, "program maybe panic"}
		ErrorResp(res, ctx)
	}
}

/*
 * name :			NewRequestMap
 * Description :	构建图片处理的参数
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-08-30
 * correcter :
 * correct date :
 * reason :
 * version :	2.004
 */
func NewRequestMap(ctx *Context, url, module string) map[string]interface{} {
	data := map[string]interface{}{
		"url":         url,
		"ns":          ctx.ns,
		"key":         ctx.key,
		"spec":        ctx.spec,
		"requestId":   ctx.requestId,
		"fsShard":     ctx.fsShard,
		"httpversion": ctx.req.Proto,
		"module":      module,
	}

	keys := []string{"opt", "width", "height", "startx", "starty", "angle", "pointsize", "color", "text", "scale", "store", "format", "method", "enlarge"}
	for _, key := range keys {
		if val, has := ctx.extend[key]; has {
			data[key] = val
		} else {
			data[key] = getDefaultValue(key)
		}
	}
	return data
}

/// 为了兼容添加的函数，由于以前spec里没有scale,所以默认是不支持等比，以前spec没有enlarge，默认是支持放大的
func getDefaultValue(key string) interface{} {
	switch key {
	case "scale":
		return 0
	case "enlarge":
		return 1
	case "format":
		fallthrough
	case "method":
		return ""
	}
	return nil
}

/*
 * name :			GenerateUrl
 * Description :	生成url
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-08-09
 * correcter :
 * correct date :
 * reason :
 * version :	2.002
 */
func GenerateUrl(ns, key, spec string) (httpUrl string, httpsUrl string, err error) {
	if GetDomain(ns) == "" {
		return "", "", errors.New("no domain is configured")
	}

	if NeedSecurity(ns) {
		return GenPrivateURL(ns, key, spec)
	} else {
		return GenPublicURL(ns, key, spec)
	}
}

/*
 * name :			GenPublicURL
 * Description :	生成publicUrl
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-08-09
 * correcter :
 * correct date :
 * reason :
 * version :	2.002
 */
func GenPublicURL(ns, key, spec string) (httpUrl, httpsUrl string, err error) {
	items := strings.Split(key, "/")
	for index, item := range items {
		item = url.QueryEscape(item)
		items[index] = item
	}
	key = strings.Join(items, "/")
	ext := filepath.Ext(key)
	suffix := ""
	if spec != Origin {
		suffix = fmt.Sprintf("!%s%s", spec, ext)
	}
	urlStr := fmt.Sprintf("%s/static/%s/%s%s", GetDomain(ns), ns, key, suffix)
	httpUrl = "http://" + urlStr
	httpsUrl = "https://" + urlStr
	return
}

/*
 * name :			GenPrivateURL
 * Description :	生成privateUrl
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-08-09
 * correcter :
 * correct date :
 * reason :
 * version :	2.002
 */
func GenPrivateURL(ns, key, spec string) (httpUrl string, httpsUrl string, err error) {
	items := strings.Split(key, "/")
	for index, item := range items {
		item = url.QueryEscape(item)
		items[index] = item
	}
	key = strings.Join(items, "/")
	ext := filepath.Ext(key)

	specExt := ""
	if spec != Origin {
		specExt = fmt.Sprintf("!%s%s", spec, ext)
	}
	expire := strconv.FormatInt(time.Now().Unix()+GetNsTimeout(ns), 10)
	path := fmt.Sprintf("/static/%s/%s%s", ns, key, specExt)
	strToSign := fmt.Sprintf("%s?expire=%s", path, expire)

	signiture := ""
	signiture, err = hash.GenSig(strToSign, GetNsSecretKey(ns))
	if err != nil {
		return
	}
	urlStr := fmt.Sprintf("%s%s&signiture=%s", GetDomain(ns), strToSign, signiture)
	httpUrl = "http://" + urlStr
	httpsUrl = "https://" + urlStr
	return
}

func GenerateKey(ns string) string {
	var key string
	var existResources []*model.Resource

	for {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		candidates := []byte(Characters)
		buf := bytes.NewBuffer(nil)
		count := len(Characters)
		for i := 0; i < KeySize; i++ {
			index := r.Intn(count)
			buf.WriteByte(candidates[index])
		}
		key = fmt.Sprintf("%s%s", MagicNum, buf.String())

		dbIdx := ChooseOneDb(ns, key, uint32(len(config.ResourceMap)))

		existResources, _ = config.ResourceMap[dbIdx].Gets(ns, key)

		if len(existResources) > 0 {
			glog.Infof("regen random key !!!!!")
		} else {
			return key
		}
	}

	return ""
}

func GenerateArgs(lifecycle string) url.Values {
	args := url.Values{}
	nslifecycle := NsLifeCycle{}

	if err := json.Unmarshal([]byte(lifecycle), &nslifecycle); err != nil {
		return args
	}
	args.Add("ttl", nslifecycle.TTL)

	return args
}

func ChooseOneDb(ns string, key string, totalDbNum uint32) int {
	key = fmt.Sprintf("%s#%s", ns, key)
	hashValue := crc32.ChecksumIEEE([]byte(key))

	return int(hashValue % totalDbNum)
}

func SelectMds(ns, key string) string {
	if len(config.MDSs) == 0 {
		return ""
	}
	key = fmt.Sprintf("%s#%s", ns, key)
	PG_ID := Crush_hash_pg_id(1,key)//只有一个池
	// pos := int(hashValue) % len(config.MDSs)//其实这里等价于MasterMds
	pos := Repeat_Repeat_choose(PG_ID)
	//log.Printf("choose the mds %v\n",pos)
	return pos

}

func SelectDisk(ns, key string) int {
	mdss := strings.Split(config.Mds, ",")
	if len(mdss) == 0 {
		return -1
	}

	key = fmt.Sprintf("%s#%s", ns, key)
	hashValue := crc32.ChecksumIEEE([]byte(key))
	pos := int(hashValue) % len(mdss)

	return pos
}
