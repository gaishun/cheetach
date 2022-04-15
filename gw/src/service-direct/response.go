package service_direct

import (
	"bytes"
	"../config"
	"errors"
	"fmt"
	"io"
	"lib/glog"
	"mime"
	"net/http"
	"pkg/httputil"
	"pkg/mimeutil"
	"./fs"
	"./model"
	"strconv"
	"time"
)

func WriteOptionsResponse(w http.ResponseWriter, ctx *Context) {
	origin := ctx.req.Header.Get("Origin")

	if origin == "" {
		origin = "*"
	}

	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Access-Control-Allow-Methods", "HEAD,GET,PUT,POST,DELETE")
	w.Header().Set("Access-Control-Allow-Credentials", "true")

	w.WriteHeader(http.StatusOK)
}

func WriteSuccResponseForHeadObject(w http.ResponseWriter, ctx *Context) {
	switch ctx.resource.FsType {
	case DFS:
		SuccRespHeadFile(w, ctx)
	case MDFS:
		SetCommonRespHeaders(w, ctx)
	}
}

func WriteSuccResponseForGetObject(w http.ResponseWriter, ctx *Context) {
	switch ctx.resource.FsType {
	case DFS:
		WriteDfsResponse(w, ctx)
	case MDFS:
		WriteMdfsResponse(w, ctx)
	}
}

func WriteDfsResponse(w http.ResponseWriter, ctx *Context) {
	SuccRespGetFile(w, ctx)
}

func WriteImageResponse(w http.ResponseWriter, ctx *Context, data []byte, mType string) {
	statusCode := checkCache(ctx.req, ctx)
	if statusCode == http.StatusNotModified {
		SetCommonRespHeaders(w, ctx)
		w.WriteHeader(statusCode)
		return
	}

	SetCommonRespHeaders(w, ctx)
	w.Header().Del("Accept-Ranges")
	w.Header().Set("Content-Type", mType)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	_, err := io.Copy(w, bytes.NewBuffer(data))
	if err != nil {
		glog.Errorf("io copy error: %v", err)
	}
}

func WriteMdfsResponse(w http.ResponseWriter, ctx *Context) {
	statusCode := checkCache(ctx.req, ctx)
	if statusCode == http.StatusNotModified {
		SetCommonRespHeaders(w, ctx)
		w.WriteHeader(statusCode)
		return
	}

	var err error
	httpRange := &httputil.Range{
		Start:  0,
		Length: ctx.resource.FileSize,
	}

	rangeReq := ctx.req.Header.Get("Range")
	if rangeReq != "" {
		httpRange, err = httputil.CheckRange(rangeReq, ctx.resource.FileSize)
		if err != nil {
			ctx.result = &Result{InvalidRange, err.Error()}
			ErrorResp(w, ctx)
			return
		}
	}

	dbIdx := ChooseOneDb(ctx.ns, ctx.key, uint32(len(config.ResourceMap)))
	// get all parts
	objectParts, err := config.ObjectPartMap[dbIdx].GetObjectParts(ctx.resource.Ns, ctx.resource.Key, ctx.resource.UploadID)
	if err != nil {
		ctx.result = &Result{InternalError, err.Error()}
		ErrorResp(w, ctx)
		return
	}

	// get seaweedfs return content-type
	if mime.TypeByExtension(ctx.resource.OriginFileName) == "" && len(objectParts) > 1 {
		dfsRs, err := getDfsObjectPart(objectParts[0], "")
		if err == nil {
			defer dfsRs.Body.Close()
			ctx.weedMimeType = dfsRs.Header.Get("content-type")
		}
	}

	SetCommonRespHeaders(w, ctx)
	w.Header().Set("Content-Length", strconv.FormatInt(httpRange.Length, 10))
	if rangeReq != "" {
		w.Header().Set("Content-Range", httpRange.ContentRange(ctx.resource.FileSize))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	var (
		partPoint int64
		rStart    = httpRange.Start
		rEnd      = httpRange.Start + httpRange.Length
	)

	for _, objectPart := range objectParts {
		if partPoint >= rEnd {
			break
		}

		if (partPoint + objectPart.FileSize - 1) < rStart {
			partPoint += objectPart.FileSize
			continue
		}

		partOffset := rStart - partPoint
		partLen := objectPart.FileSize - partOffset
		if objectPart.FileSize+partPoint > rEnd {
			partLen = rEnd - rStart
		}

		// write part data
		_, err = WriteMultiData(w, objectPart, partOffset, partLen)
		if err != nil {
			return
		}
		rStart += partLen
		partPoint += objectPart.FileSize
	}
}

func WriteMultiData(w http.ResponseWriter, objectPart *model.ObjectPart, offset, len int64) (int64, error) {
	dRange := fmt.Sprintf("bytes=%d-%d", offset, offset+len-1)
	dfsRs, err := getDfsObjectPart(objectPart, dRange)
	if err != nil {
		return 0, err
	}
	defer dfsRs.Body.Close()

	return io.Copy(w, dfsRs.Body)
}

func SetCommonRespHeaders(w http.ResponseWriter, ctx *Context) {
	w.Header().Set("content-type", mimeutil.TypeByExtension(ctx.resource.OriginFileName))
	if ctx.weedMimeType != "" {
		w.Header().Set("content-type", ctx.weedMimeType)
	}

	contentDisposition := "inline"
	if ctx.req.FormValue("attach") != "" {
		if attach, _ := strconv.ParseBool(ctx.req.FormValue("attach")); attach {
			contentDisposition = "attachment"
		}
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("%s; filename=%s", contentDisposition, ctx.resource.OriginFileName))

	if disposition := ctx.req.FormValue("response-content-disposition"); disposition != "" {
		w.Header().Set("content-disposition", disposition)
	}
	if contentType := ctx.req.FormValue("response-content-type"); contentType != "" {
		w.Header().Set("content-type", contentType)
	}

	w.Header().Set("Content-Length", strconv.FormatInt(ctx.resource.FileSize, 10))
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("etag", ctx.resource.Md5)
	w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%d", ctx.maxAge))
	w.Header().Set("x-request-id", ctx.requestId)
	w.Header().Set("x-gift-server", ctx.host)
	w.Header().Set("Last-Modified", time.Unix(ctx.resource.CreationTime, 0).UTC().Format(http.TimeFormat))
}

// check http cache
func checkCache(r *http.Request, ctx *Context) int {
	if r.Header.Get("If-Modified-Since") != "" {
		if t, parseError := time.Parse(http.TimeFormat, r.Header.Get("If-Modified-Since")); parseError == nil {
			if t.Unix() >= ctx.resource.CreationTime {
				return http.StatusNotModified
			}
		}
	}

	if inm := r.Header.Get("If-None-Match"); inm != "" && inm == ctx.resource.Md5 {
		return http.StatusNotModified
	}

	return http.StatusOK
}

func getDfsObjectPart(objectPart *model.ObjectPart, dRange string) (*http.Response, error) {
	dfs, err := fs.NewDfsClient(objectPart.FsShard)
	if err != nil {
		return nil, err
	}

	header := http.Header{}
	if dRange != "" {
		header.Set("Range", dRange)
	}

	dfsRs, err := dfs.Get(header, objectPart.FsKey)
	if err != nil {
		return nil, err
	}
	if dfsRs == nil {
		return nil, errors.New("http response body is nil")
	}

	return dfsRs, nil
}
