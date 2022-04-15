package service_direct

import (
	"../config"
	"./fs"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"pkg/util"
	"strconv"
	"strings"
	"time"
	//"context"
)

var GIFTContentType = map[string]string{
	"xls":  "application/vnd.ms-excel",
	"png":  "image/png",
	"gif":  "image/gif",
	"js":   "application/javascript",
	"css":  "text/css",
	"svg":  "image/svg+xml",
	"tif":  "image/tiff",
	"html": "text/html",
	"htx":  "text/html",
	"txt":  "text/plain",
	"jpg":  "image/jpeg",
	"ico":  "application/x-ico",
	"swf":  "application/x-shockwave-flash",
	"jpeg": "image/jpeg",
	"pdf":  "application/pdf",
	"apk":  "application/vnd.android.package-archive",
	"mp3":  "audio/mp3",
	"mp4":  "video/mpeg4",
	"jpe":  "image/jpeg",
	"doc":  "application/msword",
	"csv":  "text/csv",
	"json": "application/json",
	"webp": "image/webp",
}

/*
 * name :			DownloadHandler
 * Description :	下载的入口函数
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
func DownloadHandler(res http.ResponseWriter, req *http.Request) {

	// 创建上下文件,入口打印日志
	context := prepare(req, "/static/")
	context.odin = MetricDownload
	defer Recovery(res, context)

	// 处理GET和HEAD
	switch req.Method {
	case "OPTIONS":
		WriteOptionsResponse(res, context)
		return
	case "GET":
		// GetFileLogic(context)
		GetFileLogicGrpc(context)
	case "HEAD":
		// GetFileLogic(context)
		GetFileLogicGrpc(context)
	default:
		context.result = &Result{InvalidMethod, "method error! only support GET|HEAD"}
	}

	// 回复结果
	if context.result.ErrorCode == Success && req.Method == "GET" {
		// WriteSuccResponseForGetObject(res, context)
		SuccRespGetFileGrpc(res, context)
	} else if context.result.ErrorCode == Success && req.Method == "HEAD" {
		// WriteSuccResponseForHeadObject(res, context)
		SuccRespHeadFileGrpc(res, context)
	} else {
		ErrorResp(res, context)
	}

	// 出口打印日志
	context.processTime = time.Since(context.start).Nanoseconds() / time.Millisecond.Nanoseconds()
	//glog.Infof(LOGTEMPLATE, "end",
	//	context.requestId,
	//	context.host,
	//	req.RemoteAddr,
	//	req.Method,
	//	req.RequestURI,
	//	context.ns,
	//	context.key,
	//	context.fsShard,
	//	context.fsPublicUrl,
	//	context.fsKey,
	//	context.resource.FsType,
	//	context.result.ErrorCode.StatusCode,
	//	context.processTime,
	//	context.resource.FileSize,
	//	context.result.ErrorMsg,
	//)
	context.odin(DOWNLOAD_CALLER, context)
}

/*
 * name :			GetFileLogic
 * Description :	获取文件处理逻辑
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
func GetFileLogic(ctx *Context) {
	// ns是否存在
	nsConf, has := GetNamespace(ctx.ns)
	if !has {
		ctx.result = &Result{NamespaceNotExist, "namespace not exit"}
		return
	}
	// qps限制
	curQps := config.QpsLimit.Count(ctx.ns, 1)
	if !config.QpsLimit.CanPass(ctx.ns, nsConf.MaxQps) {
		ErrorMsg := fmt.Sprintf("MaxQps=[%v] CurQps=[%v] Download Qps Over Load", nsConf.MaxQps, curQps)
		ctx.result = &Result{RequsetTooFrequently, ErrorMsg}
		return
	}

	// 检查是否在白名单内
	ip := IP(ctx.req)
	if !CanPassWhiteList(ctx.ns, ip) {
		ctx.result = &Result{InvalidOrigin, fmt.Sprintf("%s is not on the white list", ip)}
		return
	}

	// key判空
	if ctx.key == "" {
		ctx.result = &Result{ResourceNotExist, "resourceKey can't be null"}
		return
	}
	// 验证token、更新maxAge
	ctx.maxAge = GetMaxAge(ctx.ns)
	if NeedSecurity(ctx.ns) {
		sig, expire, err := util.ValidateToken(ctx.req, ctx.ns, GetNsSecretKey(ctx.ns))
		if !sig || expire && err != nil {
			ctx.result = &Result{InvalidToken, "token auth failure: " + err.Error()}
			return
		}
		expireTime, err := strconv.Atoi(ctx.req.FormValue("expire"))
		if err == nil {
			diff := int64(expireTime) - time.Now().Unix()
			if diff < 0 {
				diff = 0
			}
			ctx.maxAge = diff
		}
	}

	dbIdx := ChooseOneDb(ctx.ns, ctx.key, uint32(len(config.ResourceMap)))
	// get resource from db
	resource, err := config.ResourceMap[dbIdx].Get(ctx.ns, ctx.key, ctx.spec)
	if err != nil {
		ctx.result = &Result{ResourceNotExist, err.Error()}
		// 验证是否需要处理图片
		if len(GetDefaultSpec(ctx.ns)) != 0 {

		}
		return
	}

	// check object ttl
	if IsLifeCycle(ctx.ns) {
		ttl := LifeCycle(ctx.ns)
		if ttl != 0 && resource.CreationTime+ttl < time.Now().Unix() {
			ctx.result = &Result{ResourceNotExist, "resource not exist"}
			return
		}
	}

	// 获取 Gift-Fs的文
	ctx.resource = *resource
	ctx.fsShard = ctx.resource.FsShard
	ctx.fsKey = ctx.resource.FsKey
	ctx.createTime = ctx.resource.CreationTime
	if ctx.resource.FsType == DFS {
		getFsFile(ctx)
	}

	return
}

func getFsFile(ctx *Context) {
	// new weedo client
	fsCli, err := fs.NewDfsClient(ctx.fsShard)
	if err != nil {
		ctx.result = &Result{InternalError, "new weedoClient error: " + err.Error()}
		return
	}

	ctx.fsPublicUrl, ctx.fsUrl, err = fsCli.GetUrl(ctx.fsKey)
	if err != nil || ctx.fsPublicUrl == "" || ctx.fsUrl == "" {
		ctx.result = &Result{InternalError, "get fskey url failure: " + err.Error()}
		return
	}

	// 获取文件
	req, err := http.NewRequest("GET", ctx.fsPublicUrl, nil)
	if err != nil {
		ctx.result = &Result{InternalError, "new http request failure"}
		return
	}
	// 节省http流量
	since := ctx.req.Header.Get("If-Modified-Since")
	etag := ctx.req.Header.Get("If-None-Match")
	if since != "" {
		req.Header.Add("If-Modified-Since", since)
	}
	if etag != "" {
		req.Header.Add("If-None-Match", etag)
	}
	contentRange := ctx.req.Header.Get("Range")
	if contentRange != "" {
		req.Header.Add("Range", contentRange)
	}

	client := &http.Client{}
	ctx.fsResp, err = client.Do(req)
	if err != nil {
		ctx.result = &Result{InternalError, "get gift-fs file failure: " + err.Error()}
		return
	}

	if ctx.fsResp.StatusCode == http.StatusNotFound {
		ctx.result = &Result{ResourceNotExist, "resource not exist"}
		return
	}

	if ctx.fsResp == nil {
		ctx.result = &Result{InternalError, "get gift-fs file failure: response body is null!"}
		return
	}

	return
}

/*
 * name :			SuccRespGetFile
 * Description :	成功回复信息
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
func SuccRespGetFile(res http.ResponseWriter, ctx *Context) {
	var err error
	if ctx.fsResp != nil && ctx.fsResp.Body != nil {
		defer ctx.fsResp.Body.Close()
	}

	// 历史遗留问题
	fixHeader(ctx.fsResp, res, ctx)
	// 增加回复头
	for _, c := range ctx.fsResp.Cookies() {
		res.Header().Add("Set-Cookie", c.Raw)
	}

	res.Header().Set("Cache-Control", fmt.Sprintf("max-age=%d", ctx.maxAge))
	res.Header().Set("x-request-id", ctx.requestId)
	res.Header().Set("x-gift-server", ctx.host)
	res.Header().Set("Access-Control-Allow-Origin", "*")

	res.WriteHeader(ctx.fsResp.StatusCode)
	if ctx.fsResp.StatusCode == http.StatusNotModified {
		ctx.result.ErrorCode = NotModified
	}
	ctx.size, err = io.Copy(res, ctx.fsResp.Body)
	if err != nil {
		ctx.result = &Result{IOCopyFailure, "io.Copy fail: " + err.Error()}
		return
	}

	return
}

func SuccRespHeadFile(res http.ResponseWriter, ctx *Context) {
	if ctx.fsResp != nil && ctx.fsResp.Body != nil {
		defer ctx.fsResp.Body.Close()
	}
	// 历史遗留问题
	fixHeader(ctx.fsResp, res, ctx)
	// 增加回复头
	for _, c := range ctx.fsResp.Cookies() {
		res.Header().Add("Set-Cookie", c.Raw)
	}
	size, err := strconv.Atoi(res.Header().Get("Content-Length"))
	if err == nil {
		ctx.size = int64(size)
	}
	res.Header().Set("Cache-Control", fmt.Sprintf("max-age=%d", ctx.maxAge))
	res.Header().Set("x-request-id", ctx.requestId)
	res.Header().Set("x-gift-server", ctx.host)
	res.Header().Set("Access-Control-Allow-Origin", "*")

	res.WriteHeader(ctx.result.ErrorCode.StatusCode)
	res.Write(nil)

	return
}

/*
 * name :			GetFsKey
 * Description :	查询文件的fskey,加入重试逻辑
 * input :
 * output :
 * return :
 * creator :		ShiMingYa
 * creat date :		2017-07-27
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func GetFskey(ctx *Context) (err error) {
	err = getFskey(ctx)
	if err != nil {
		return getFskey(ctx)
	}
	return nil
}

func getFskey(ctx *Context) (err error) {
	if ctx.spec == "" {
		ctx.spec = Origin
	}
	region := GetNSRegion(ctx.ns)
	reqHost := config.RequireConfigData().GetMappingService(region)
	requestUrl := fmt.Sprintf("http://%s/fskey/%s/%s?specs=%s", reqHost, ctx.ns, ctx.key, ctx.spec)
	res, err := http.Get(requestUrl)
	if err != nil {
		return err
	}
	if res.Body != nil {
		defer res.Body.Close()
	}

	content, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	jsonData := make(map[string]interface{})
	err = json.Unmarshal(content, &jsonData)
	if err != nil {
		return err
	}
	status := jsonData["status"]
	if status != "success" {
		return errors.New("query fskey error")
	}

	results := getMap(jsonData, "result")
	result := getMap(results, ctx.spec)
	if result == nil || len(result) == 0 {
		return errors.New("resource not exit")
	}

	// interface{} int ---> float64
	if obj, is := result["fs_shard"].(float64); is {
		ctx.fsShard = int(obj)
	}
	if obj, is := result["fs_key"].(string); is {
		ctx.fsKey = obj
	}
	if obj, is := result["creation_time"].(float64); is {
		ctx.createTime = int64(obj)
	}
	if ctx.fsKey == "" {
		return errors.New("fskey is null")
	}

	return nil
}

func getMap(m map[string]interface{}, key string) map[string]interface{} {
	if _, has := m[key]; !has {
		return nil
	}
	if obj, is := m[key].(map[string]interface{}); is {
		return map[string]interface{}(obj)
	}
	return nil
}

/*
 * name :			fixHeader
 * Description :	历史遗留问题，修复0.70版本的seaweedfs文件mime错误
 * input :
 * output :
 * return :
 * creator :		ShiMingYa
 * creat date :		2017-07-27
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func fixHeader(resp *http.Response, w http.ResponseWriter, c *Context) {
	for k, v := range resp.Header {
		if c.createTime < 1498838400 {
			if k == "Content-Disposition" {
				strs := strings.Split(c.key, "/")
				vv := "inline; filename=\"" + strs[len(strs)-1] + "\""
				w.Header().Add(k, vv)
			} else if k == "Content-Type" {
				for _, vv := range v {
					strs := strings.Split(c.key, ".")
					if len(strs) <= 1 {
						w.Header().Add(k, vv)
					} else {
						mime := GIFTContentType[strs[len(strs)-1]]
						if mime == "" {
							w.Header().Add(k, vv)
						} else {
							w.Header().Add(k, mime)
						}
					}
				}
			} else {
				for _, vv := range v {
					w.Header().Add(k, vv)
				}
			}
		} else {
			for _, vv := range v {
				w.Header().Add(k, vv)
			}
		}
	}
}
