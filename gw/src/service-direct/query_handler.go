package service_direct

import (
	"../config"
	"errors"
	"fmt"
	"lib/glog"
	"net/http"
	"./fs"
	"./model"
	"strings"
	"time"
)

/*
 * name :			QueryHandler
 * Description :	查询的入口函数
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-08-06
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func QueryHandler(res http.ResponseWriter, req *http.Request) {
	// 创建上下文件,入口打印日志
	context := prepare(req, "/resource/")
	context.odin = MetricQuery
	defer Recovery(res, context)

	switch req.Method {
	case "OPTIONS":
		WriteOptionsResponse(res, context)
		return
	case "GET":
		QueryFileLogic(context)
	case "DELETE":
		// DeleteFileLogic(context)
		DeleteFileLogicGrpc(context)
	default:
		context.result = &Result{InvalidMethod, "method error! only support GET|DELETE"}
	}

	// 回复结果
	if context.result.ErrorCode == Success && req.Method == "GET" {
		SuccRespQueryFile(res, context)
	} else if context.result.ErrorCode == Success && req.Method == "DELETE" {
		SuccRespDeleteFile(res, context)
	} else {
		ErrorResp(res, context)
	}

	context.processTime = time.Since(context.start).Nanoseconds() / time.Millisecond.Nanoseconds()
	// 出口打印日志
	glog.Infof(LOGTEMPLATE, "end",
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
		context.fsType,
		context.result.ErrorCode.StatusCode,
		context.processTime,
		context.size,
		context.result.ErrorMsg,
	)
	context.odin(QUERY_CALLER, context)
}

/*
 * name :			QueryFsKey
 * Description :	查询FSKey的入口函数
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-07-27
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func QueryFsKey(res http.ResponseWriter, req *http.Request) {
	context := prepare(req, "/fskey/")
	context.odin = MetricFskey
	defer Recovery(res, context)

	switch req.Method {
	case "OPTIONS":
		WriteOptionsResponse(res, context)
		return
	case "GET":
		QueryFskeyLogic(context)
	default:
		context.result = &Result{InvalidMethod, "method error! only support GET"}
	}

	// 回复结果
	if context.result.ErrorCode == Success {
		SuccRespQueryFskey(res, context)
	} else {
		ErrorResp(res, context)
	}

	context.processTime = time.Since(context.start).Nanoseconds() / time.Millisecond.Nanoseconds()
	// 出口打印日志
	glog.Infof(LOGTEMPLATE, "end",
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
		context.fsType,
		context.result.ErrorCode.StatusCode,
		context.processTime,
		context.size,
		context.result.ErrorMsg,
	)
	context.odin(FSKEY_CALLER, context)
}

/*
 * name :			BatchQuery
 * Description :	批量查询的入口函数
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-07-27
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func BatchQuery(res http.ResponseWriter, req *http.Request) {
	context := prepare(req, "/batch/")
	context.odin = MetricBatch
	defer Recovery(res, context)

	switch req.Method {
	case "OPTIONS":
		WriteOptionsResponse(res, context)
		return
	case "GET":
		BatchQueryLogic(context)
	default:
		context.result = &Result{InvalidMethod, "method error! only support GET"}
	}

	// 回复结果
	if context.result.ErrorCode == Success {
		SuccRespBatchQuery(res, context)
	} else {
		ErrorResp(res, context)
	}

	context.processTime = time.Since(context.start).Nanoseconds() / time.Millisecond.Nanoseconds()
	// 出口打印日志
	glog.Infof(LOGTEMPLATE, "end",
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
		context.fsType,
		context.result.ErrorCode.StatusCode,
		context.processTime,
		context.size,
		context.result.ErrorMsg,
	)
	context.odin(BATCHQUERY_CALLER, context)
}

/*
 * name :			QueryFileLogic
 * Description :	查询单条记录
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-07-27
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func QueryFileLogic(ctx *Context) {
	// 检查是否在白名单内
	ip := IP(ctx.req)
	if !CanPassWhiteList(ctx.ns, ip) {
		ctx.result = &Result{InvalidOrigin, fmt.Sprintf("%s is not on the white list", ip)}
		return
	}

	// 获取spce
	ctx.spec = ctx.req.FormValue("spec")
	if ctx.spec == "" {
		ctx.spec = Origin
	}

	// 检验ns、key
	if ctx.ns == "" {
		ctx.result = &Result{NamespaceNotExist, "namespace can't be null"}
		return
	}
	if ctx.key == "" {
		ctx.result = &Result{ResourceNotExist, "resourceKey can't be null"}
		return
	}
	if !NamespaceExist(ctx.ns) {
		ctx.result = &Result{NamespaceNotExist, "namespace not exit"}
		return
	}
	// 验证region
	region := config.GetRegion()
	nsRegion := GetNSRegion(ctx.ns)
	if region != nsRegion {
		errmsg := fmt.Sprintf("cross region access is not allowed, the namespace is in %s, but the service-direct is for %s", nsRegion, region)
		ctx.result = &Result{InvalidRegion, errmsg}
	}

	dbIdx := ChooseOneDb(ctx.ns, ctx.key, uint32(len(config.ResourceMap)))

	// 查询资源
	resource, err := config.ResourceMap[dbIdx].Get(ctx.ns, ctx.key, ctx.spec)
	if err != nil {
		ctx.result = &Result{ResourceNotExist, err.Error()}
		return
	}
	ctx.fsType = resource.FsType
	if resource.FileSize == -1 {
		ctx.result = &Result{ImgProcessing, "imager is processing"}
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

	// 生成URL
	httpUrl, httpsUrl, err := GenerateUrl(ctx.ns, ctx.key, ctx.spec)
	if err != nil {
		ctx.result = &Result{InternalError, "can't generate url"}
		return
	}
	ctx.extend["httpUrl"] = httpUrl
	ctx.extend["httpsUrl"] = httpsUrl
	ctx.md5 = resource.Md5
	ctx.size = resource.FileSize
	ctx.createTime = resource.CreationTime
	ctx.filename = resource.OriginFileName

	return
}

func SuccRespQueryFile(res http.ResponseWriter, ctx *Context) {
	SuccessResp(res, map[string]interface{}{
		"download_url":       ctx.extend["httpUrl"],
		"download_url_https": ctx.extend["httpsUrl"],
		"md5":                ctx.md5,
		"file_size":          ctx.size,
		"creation_time":      ctx.createTime,
	}, ctx)
	return
}

/*
 * name :			DeleteFileLogic
 * Description :	删除文件
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-07-27
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func DeleteFileLogic(ctx *Context) {
	// 检查是否在白名单内
	ip := IP(ctx.req)
	if !CanPassWhiteList(ctx.ns, ip) {
		ctx.result = &Result{InvalidOrigin, fmt.Sprintf("%s is not on the white list", ip)}
		return
	}

	// 检验ns、key
	if ctx.ns == "" {
		ctx.result = &Result{NamespaceNotExist, "namespace can't be null"}
		return
	}
	if ctx.key == "" {
		ctx.result = &Result{ResourceNotExist, "resourceKey can't be null"}
		return
	}
	if !NamespaceExist(ctx.ns) {
		ctx.result = &Result{NamespaceNotExist, "namespace not exit"}
		return
	}
	// 验证region
	region := config.GetRegion()
	nsRegion := GetNSRegion(ctx.ns)
	if region != nsRegion {
		errmsg := fmt.Sprintf("cross region access is not allowed, the namespace is in %s, but the service-direct is for %s", nsRegion, region)
		ctx.result = &Result{InvalidRegion, errmsg}
	}

	dbIdx := ChooseOneDb(ctx.ns, ctx.key, uint32(len(config.ResourceMap)))

	resources, err := config.ResourceMap[dbIdx].Gets(ctx.ns, ctx.key)
	if err != nil {
		ctx.result = &Result{InternalError, "error to get resource"}
		return
	}
	if len(resources) == 0 {
		ctx.result = &Result{ResourceNotExist, "no resourse found"}
		return
	}

	ctx.fsType = resources[0].FsType
	ctx.fsShard = resources[0].FsShard
	fsCli, err := fs.NewDfsClient(ctx.fsShard)
	if err != nil {
		ctx.result = &Result{InternalError, "new weedo client error: " + err.Error()}
		return
	}

	// 删除资源
	err = deleteAllResource(resources, fsCli, dbIdx)
	if err != nil {
		ctx.result = &Result{InternalError, "delete resource failure: " + err.Error()}
		return
	}

	return
}

func SuccRespDeleteFile(res http.ResponseWriter, ctx *Context) {
	data := make(map[string]interface{})
	SuccessResp(res, data, ctx)
}

/*
 * name :			QueryFskeyLogic
 * Description :	查询文件的fsKey
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-07-27
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func QueryFskeyLogic(ctx *Context) {
	ctx.spec = ctx.req.FormValue("specs")
	if ctx.spec == "" {
		ctx.spec = Origin
	}
	// 检查ns、key
	if ctx.ns == "" {
		ctx.result = &Result{NamespaceNotExist, "namespace can't be null"}
		return
	}
	if ctx.key == "" {
		ctx.result = &Result{ResourceNotExist, "resourceKey can't be null"}
		return
	}
	if !NamespaceExist(ctx.ns) {
		ctx.result = &Result{NamespaceNotExist, "namespace not exist"}
		return
	}
	// 检查region
	region := config.GetRegion()
	nsRegion := GetNSRegion(ctx.ns)
	if region != nsRegion {
		errmsg := fmt.Sprintf("cross region access is not allowed, the namespace is in %s, but the service-direct is for %s", nsRegion, region)
		ctx.result = &Result{InvalidRegion, errmsg}
		return
	}

	// 查询多个spec
	specList := strings.Split(ctx.spec, ",")
	resourceList, err := batchQueryResources(ctx.ns, []string{ctx.key}, specList)
	if err != nil {
		ctx.result = &Result{InternalError, "query fskey failure: " + err.Error()}
		return
	}

	for _, item := range resourceList {
		result := make(map[string]interface{})
		result["namespace"] = ctx.ns
		result["key"] = ctx.key
		result["spec"] = item.Spec
		if item.FileSize == -1 {
			result["status"] = "failure"
			result["status_code"] = ImgProcessing.StatusCode
			result["message"] = "image is processing"
			ctx.extend[item.Spec] = result
			continue
		}
		result["fs_shard"] = item.FsShard
		ctx.fsType = item.FsType
		ctx.fsKey = item.FsKey
		if item.FsKey == "" {
			ctx.fsKey = "multifskey"
		}
		result["fs_key"] = ctx.fsKey
		result["md5"] = item.Md5
		result["creation_time"] = item.CreationTime
		ctx.extend[item.Spec] = result
	}

	return
}

func SuccRespQueryFskey(res http.ResponseWriter, ctx *Context) {
	SuccessResp(res, map[string]interface{}{
		"result": ctx.extend,
	}, ctx)
}

/*
 * name :			batchQuery
 * Description :	批量查询
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
func BatchQueryLogic(ctx *Context) {
	_, ctx.ns, ctx.key = extractUrl(ctx.req.URL.Path, "/batch/")
	// 检查ns
	if ctx.ns == "" {
		ctx.result = &Result{NamespaceNotExist, "namespace can't be null"}
		return
	}
	if !NamespaceExist(ctx.ns) {
		ctx.result = &Result{NamespaceNotExist, "namespace not exit"}
		return
	}
	// 检查region
	region := config.GetRegion()
	nsRegion := GetNSRegion(ctx.ns)
	if region != nsRegion {
		errmsg := fmt.Sprintf("cross region access is not allowed, the namespace is in %s, but the service-direct is for %s", nsRegion, region)
		ctx.result = &Result{InvalidRegion, errmsg}
		return
	}

	// 获取key
	ctx.key = strings.TrimSpace(ctx.req.FormValue("keys"))
	ctx.key = strings.Trim(ctx.key, ",")
	if ctx.key == "" {
		ctx.result = &Result{ResourceNotExist, "resourceKey can't be null"}
		return
	}
	keys := strings.Split(ctx.key, ",")
	if len(keys) > MaxQueryBatchSize {
		errmsg := fmt.Sprintf("incorrect key count, max batch size is %d", MaxQueryBatchSize)
		ctx.result = &Result{InvalidArgument, errmsg}
		return
	}
	// 获取spec
	ctx.spec = strings.TrimSpace(ctx.req.FormValue("specs"))
	ctx.spec = strings.Trim(ctx.spec, ",")
	if ctx.spec == "" {
		ctx.spec = Origin
	}
	// 批量查询
	specs := strings.Split(ctx.spec, ",")
	results, err := batchQueryResources(ctx.ns, keys, specs)
	if err != nil {
		ctx.result = &Result{InternalError, "batch query failure: " + err.Error()}
		return
	}

	// 构造返回结果
	data, err := constructBatchQueryResult(ctx.ns, keys, specs, results)
	if err != nil {
		ctx.result = &Result{InternalError, "construct result fail: " + err.Error()}
		return
	}
	ctx.extend["result"] = data

	return
}

func SuccRespBatchQuery(res http.ResponseWriter, ctx *Context) {
	SuccessResp(res, ctx.extend, ctx)
}

/*
 * name :			batchQueryResources
 * Description :	批量查询多个key和spec
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-07-27
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func batchQueryResources(ns string, keys []string, specs []string) ([]*model.Resource, error) {
	results := []*model.Resource{}
	for _, key := range keys {

		dbIdx := ChooseOneDb(ns, key, uint32(len(config.ResourceMap)))

		resources, e := config.ResourceMap[dbIdx].Gets(ns, key)
		if e != nil {
			return nil, errors.New("error occurs when loading resource")
		}
		for _, res := range resources {
			// check object ttl
			if IsLifeCycle(ns) {
				ttl := LifeCycle(ns)
				if ttl != 0 && res.CreationTime+ttl < time.Now().Unix() {
					continue
				}
			}
			for _, item := range specs {
				if item == res.Spec {
					results = append(results, res)
				}
			}
		}
	}
	return results, nil
}

/*
 * name :			constructBatchQueryResult
 * Description :	构造查询返回结果
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-07-27
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func constructBatchQueryResult(ns string, keys []string, specs []string, results []*model.Resource) (ret []interface{}, err error) {
	ret = []interface{}{}
	getResult := func(key string, spec string) *model.Resource {
		for _, item := range results {
			if item.Key == key && item.Spec == spec {
				return item
			}
		}
		return nil
	}
	for _, key := range keys {
		for _, spec := range specs {
			singleJson := map[string]interface{}{
				"namespace": ns,
				"key":       key,
				"spec":      spec,
			}
			item := getResult(key, spec)
			// 资源不不存在
			if item == nil {
				singleJson["status"] = "failure"
				singleJson["status_code"] = ResourceNotExist.StatusCode
				singleJson["message"] = "resource doesn't exist"
				ret = append(ret, singleJson)
				continue
			}
			// 图片正在处理中
			if item.FileSize == -1 {
				singleJson["status"] = "failure"
				singleJson["status_code"] = ImgProcessing.StatusCode
				singleJson["message"] = "image is processing"
				ret = append(ret, singleJson)
				continue
			}
			// 生成下载url失败则意味着生成这个ns下的所有url都会失败，直接返回
			downloadUrl, downloadHttpsUrl, err := GenerateUrl(ns, item.Key, item.Spec)
			if err != nil {
				return ret, err
			}
			singleJson["status"] = "success"
			singleJson["download_url"] = downloadUrl
			singleJson["download_url_https"] = downloadHttpsUrl
			singleJson["md5"] = item.Md5
			singleJson["file_size"] = item.FileSize
			singleJson["creation_time"] = item.CreationTime
			ret = append(ret, singleJson)
		}
	}
	return
}
