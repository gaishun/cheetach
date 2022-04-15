package service_direct

import (
	"../config"
	"fmt"
	"lib/glog"
	"net/http"
	"path/filepath"
	"pkg/hash"
	"pkg/util"
	"./fs"
	"./model"
	"strconv"
	"strings"
	"time"
)

/*
 * name :			UploadHandler
 * Description :	上传的入口函数
 * input :
 * output :
 * return :
 * creator :		ShiMingYa
 * creat date :		2017-08-03
 * correcter :
 * correct date :
 * reason :
 * version :	0.1
 */
func UploadHandler(res http.ResponseWriter, req *http.Request) {
	context := prepare(req, "/resource/")
	context.odin = MetricUpload
	defer Recovery(res, context)

	switch req.Method {
	case "OPTIONS":
		WriteOptionsResponse(res, context)
		return
	case "PUT":
		// UploadFileLogic(context)
		fmt.Println("upload grpc before: ", time.Since(context.start).Nanoseconds())
		UploadFileLogicGrpc(context)
		fmt.Println("upload grpc end: ", time.Since(context.start).Nanoseconds())
	case "POST":
		// UploadFileLogic(context)
	default:
		context.result = &Result{InvalidMethod, "method error! only support PUT|POST"}
	}

	/* 回复结果 */
	if context.result.ErrorCode == Success {
		SuccRespUploadFile(res, context)
	} else {
		ErrorResp(res, context)
	}
	context.processTime = time.Since(context.start).Nanoseconds() / time.Millisecond.Nanoseconds()
//	glog.Infof(LOGTEMPLATE, "end",
//		context.requestId,
//		context.host,
//		req.RemoteAddr,
//		req.Method,
//		req.RequestURI,
//		context.ns,
//		context.key,
//		context.fsShard,
//		context.fsPublicUrl,
//		context.fsKey,
//		context.resource.FsType,
//		context.result.ErrorCode.StatusCode,
//		context.processTime,
//		context.size,
//		context.result.ErrorMsg,
//	)
	if strings.HasPrefix(context.key, MagicNum) {
		context.odin(UPLOADWITHOUTKEY_CALLER, context)
	} else {
		context.odin(UPLOADWITHKEY_CALLER, context)
	}
}

func UploadCheck(ctx *Context) {
	ctx.reader = ctx.req.Body
}

func UploadCheck1(ctx *Context) {
	fmt.Println("upload check entry: ", time.Since(ctx.start).Nanoseconds()/time.Millisecond.Nanoseconds())
	// 验证content-type
	file, header, err := ctx.req.FormFile("filecontent")
	if err != nil {
		v := ctx.req.Header.Get("Content-Type")
		ctx.result = &Result{InvalidArgument, "file is not specified in multipart " + v + err.Error()}
		return
	}
	fmt.Println("upload check get file: ", time.Since(ctx.start).Nanoseconds()/time.Millisecond.Nanoseconds())

	ctx.mime = header.Header.Get("content-type")

	// 获取filename
	ctx.filename = strings.TrimSpace(header.Filename)
	if ctx.filename == "" {
		ctx.result = &Result{InvalidArgument, "filename is missing"}
		return
	}
	ctx.filename = filepath.Base(ctx.filename)

	// 验证文件大小和MD5
	fmt.Println("upload check md5 entry: ", time.Since(ctx.start).Nanoseconds()/time.Millisecond.Nanoseconds())
	ctx.size, ctx.md5, err = util.MD5(file)
	fmt.Println("upload check md5 end: ", time.Since(ctx.start).Nanoseconds()/time.Millisecond.Nanoseconds())
	if err != nil {
		ctx.result = &Result{InternalError, "generate file md5 error"}
		return
	}
	if ctx.size == 0 {
		ctx.result = &Result{InvalidFileSize, "can't upload empty file"}
		return
	}
	if ctx.key == "" {
		ctx.key = GenerateKeyGrpc()
	}

	ctx.reader = file
}

/*
 * name :			UploadFileLogic
 * Description :	上传文件的逻辑处理
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
func UploadFileLogic(ctx *Context) {
	// ns是否存在
	nsConf, ok := GetNamespace(ctx.ns)
	if !ok {
		ctx.result = &Result{NamespaceNotExist, "namespace not exit"}
		return
	}
	// qps限制
	curQps := config.QpsLimit.Count(ctx.ns, 1)
	if !config.QpsLimit.CanPass(ctx.ns, nsConf.MaxQps) {
		ErrorMsg := fmt.Sprintf("MaxQps=[%v] CurQps=[%v] Upload Qps Over Load", nsConf.MaxQps, curQps)
		ctx.result = &Result{RequsetTooFrequently, ErrorMsg}
		return
	}

	// 检查是否在白名单内
	ip := IP(ctx.req)
	if !CanPassWhiteList(ctx.ns, ip) {
		ctx.result = &Result{InvalidOrigin, fmt.Sprintf("%s is not on the white list", ip)}
		return
	}

	// 验证ns
	ctx.ns = strings.TrimSpace(ctx.ns)
	if err := util.ValidateNamespace(ctx.ns); err != nil {
		ctx.result = &Result{InvalidNamespace, err.Error()}
		return
	}

	// 验证key
	if ctx.key == "" {
		ctx.key = GenerateKey(ctx.ns)
	}
	ctx.key = strings.TrimSpace(ctx.key)
	if err := util.ValidateKey(ctx.key); err != nil {
		ctx.result = &Result{InvalidKey, err.Error()}
		return
	}

	// 验证region
	region := config.GetRegion()
	nsRegion := GetNSRegion(ctx.ns)
	if region != nsRegion {
		errmsg := fmt.Sprintf("cross region access is not allowed, the namespace is in %s, but the service-direct is for %s", nsRegion, region)
		ctx.result = &Result{InvalidRegion, errmsg}
		return
	}

	// 验证content-type
	fileBody := ctx.req.Body
	// fileBody, header, err := ctx.req.FormFile("filecontent")
	// if err != nil {
	// 	ctx.result = &Result{InvalidArgument, "file is not specified in multipart " + ctx.req.Header.Get("Content-Type") + err.Error()}
	// 	return
	// }
	// ctx.mime = header.Header.Get("content-type")
	// if !CanMimeAccept(ctx.ns, ctx.mime) {
	// 	ctx.result = &Result{InvalidMimeType, ctx.mime + "is not acceptable"}
	// 	return
	// }

	// 获取filename
	ctx.filename = ctx.key
	// ctx.filename = strings.TrimSpace(header.Filename)
	if ctx.filename == "" {
		ctx.result = &Result{InvalidArgument, "filename is missing"}
		return
	}
	ctx.filename = filepath.Base(ctx.filename)

	var err error
	ctx.size, err = strconv.ParseInt(ctx.req.Header.Get("content-length"), 10, 64)
	if err != nil {
		ctx.result = &Result{InternalError, fmt.Sprintf("compute file size error: %v", err)}
		return
	}
	// if ctx.size, err = util.Size(fileBody); err != nil {
	// 	ctx.result = &Result{InternalError, fmt.Sprintf("compute file size error: %v", err)}
	// 	return
	// }

	if ctx.size == 0 {
		ctx.result = &Result{InvalidFileSize, "can't upload empty file"}
		return
	}

	limitSize := config.RequireConfigData().MaxFileSize
	if nsConf.SegmentEnable {
		limitSize = int64(config.MaxSize)
	}
	if limitSize > 0 && ctx.size > limitSize {
		ctx.result = &Result{InvalidFileSize, "the file size exceeds the limit"}
		return
	}

	// select from writable gift-fs by hash crc32
	if ctx.fsShard = config.ShardFs(region, ctx.ns, ctx.key); ctx.fsShard == -1 {
		ctx.result = &Result{InternalError, "no gift-fs to use"}
		return
	}
	fsCli, err := fs.NewDfsClient(ctx.fsShard)
	if err != nil {
		ctx.result = &Result{InternalError, "new weedo client error: " + err.Error()}
		return
	}

	dbIdx := ChooseOneDb(ctx.ns, ctx.key, uint32(len(config.ResourceMap)))

	// 查询mysql中是否有相同key、spec的文件
	existRes, err := config.ResourceMap[dbIdx].Gets(ctx.ns, ctx.key)
	if err != nil {
		ctx.result = &Result{InternalError, "error to load resource from mysql: " + err.Error()}
		return
	}
	// 如果存在则先删除mysql，再删除gift-fs
	deleteAllResource(existRes, fsCli, dbIdx)

	// upload file
	hashReader, err := hash.NewReader(fileBody, ctx.req.FormValue("md5"))
	if err != nil {
		ctx.result = &Result{InvalidDigest, err.Error()}
		return
	}

	if err := uploadObject(ctx, hashReader, nsConf.LifeCycle, fsCli, dbIdx); err != nil {
		deleteAllResource([]*model.Resource{&ctx.resource}, fsCli, dbIdx)
		if IsBadDigestError(err) {
			ctx.result = &Result{InvalidDigest, "the content md5 you specified is not valid"}
		} else {
			ctx.result = &Result{FileSystemError, "upload file error: " + err.Error()}
		}
		return
	}

	// 生成url
	httpUrl, httpsUrl, err := GenerateUrl(ctx.ns, ctx.key, ctx.spec)
	if err != nil {
		ctx.result = &Result{InternalError, "can't generate download url"}
		return
	}
	ctx.extend["httpUrl"] = httpUrl
	ctx.extend["httpsUrl"] = httpsUrl

	// 图片处理
	if len(GetDefaultSpec(ctx.ns)) != 0 {
		go processImg(ctx, dbIdx)
	}

	return
}

func SuccRespUploadFile(res http.ResponseWriter, ctx *Context) {
	SuccessResp(res, map[string]interface{}{
		"resource_key":       ctx.key,
		"download_url":       ctx.extend["httpUrl"],
		"download_url_https": ctx.extend["httpsUrl"],
		"md5":                ctx.md5,
		"file_size":          ctx.size,
		"creation_time":      ctx.createTime,
	}, ctx)
}

/*
 * name :			processImg
 * Description :	离线处理图片
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
func processImg(ctx *Context, dbIdx int) {
	specs := GetDefaultSpec(ctx.ns)
	if len(specs) == 0 {
		return
	}
	value := ""
	for ctx.spec, value = range specs {
		// 不处理原图片
		if ctx.spec == Origin {
			continue
		}
		glog.Infof(config.IMGLOG, "image-start", ctx.requestId, "upload", 0, ctx.ns, ctx.key, ctx.spec, http.StatusOK, "")
		// 插入mysql记录
		_, err := config.ResourceMap[dbIdx].Put(&model.Resource{
			Ns:             ctx.ns,
			Key:            ctx.key,
			Spec:           ctx.spec,
			Mime:           ctx.mime,
			FsType:         DFS,
			FileSize:       -1,
			OriginFileName: ctx.filename,
			CreationTime:   time.Now().Unix(),
		})
		if err != nil {
			glog.Errorf(config.IMGLOG, "image-error", ctx.requestId, "upload", 0, ctx.ns, ctx.key, ctx.spec, http.StatusBadRequest, "insert mysql error: "+err.Error())
			continue
		}
		// 放入图片处理器
		ctx.extend, err = util.ExtractSpec(value)
		if err != nil {
			glog.Errorf(config.IMGLOG, "image-error", ctx.requestId, "upload", 0, ctx.ns, ctx.key, ctx.spec, http.StatusBadRequest, "parse spec error: "+err.Error())
			continue
		}
		NewRequestMap(ctx, ctx.fsPublicUrl, "upload")

	}
}
