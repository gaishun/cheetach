package service_direct

import (
	"../config"
	"io"
	"lib/uuid"
	"pkg/hash"
	"./fs"
	"./model"
	"strings"
	"time"
)

func uploadObject(ctx *Context, hashReader *hash.Reader, lifeCycle string, fsCli fs.Dfs, dbIdx int) error {
	switch {
	case !isEnableSegment(ctx.ns):
		return uploadObjectSmall(ctx, hashReader, lifeCycle, fsCli, dbIdx)
	case isEnableSegment(ctx.ns) && isSmallObject(ctx.size):
		return uploadObjectSmall(ctx, hashReader, lifeCycle, fsCli, dbIdx)
	case isEnableSegment(ctx.ns) && isLargeObject(ctx.size):
		return uploadObjectLarge(ctx, hashReader, lifeCycle, fsCli, dbIdx)
	}
	return nil
}

func uploadObjectSmall(ctx *Context, hashReader *hash.Reader, lifeCycle string, fsCli fs.Dfs, dbIdx int) (err error) {
	ctx.fsKey, _, err = fsCli.Put(ctx.filename, ctx.mime, hashReader, GenerateArgs(lifeCycle))
	if err != nil {
		ctx.result = &Result{FileSystemError, "upload file error: " + err.Error()}
		return
	}

	ctx.md5 = hashReader.MD5HexString()
	ctx.createTime = time.Now().Unix()
	ctx.resource = model.Resource{
		Ns:             ctx.ns,
		Key:            ctx.key,
		Spec:           ctx.spec,
		Mime:           ctx.mime,
		FsKey:          ctx.fsKey,
		FsType:         DFS,
		FsShard:        ctx.fsShard,
		DomainShardId:  0,
		FileSize:       ctx.size,
		OriginFileName: ctx.filename,
		Md5:            ctx.md5,
		CreationTime:   ctx.createTime,
		UploadID:       DefaultUploadID,
	}

	if ctx.fsPublicUrl, ctx.fsUrl, err = fsCli.GetUrl(ctx.fsKey); err != nil {
		ctx.result = &Result{InternalError, "get fsUrl fail: " + err.Error()}
		return
	}

	_, err = config.ResourceMap[dbIdx].Put(&ctx.resource)

	return err
}

func uploadObjectLarge(ctx *Context, hashReader *hash.Reader, lifeCycle string, fsCli fs.Dfs, dbIdx int) (err error) {
	uploadID := uuid.New()
	ctx.objectPart.BucketName = ctx.ns
	ctx.objectPart.ObjectKey = ctx.key
	ctx.objectPart.UploadId = uploadID
	ctx.objectPart.PartId = 1
	ctx.objectPart.NeedleId = 0
	ctx.objectPart.FsShard = ctx.fsShard
	ctx.objectPart.FsType = DFS
	ctx.objectPart.IsDeleted = 0
	ctx.objectPart.FileTTL = 0
	ctx.objectPart.Version = ""

	ctx.resource = model.Resource{
		Ns:             ctx.ns,
		Key:            ctx.key,
		Spec:           ctx.spec,
		Mime:           ctx.mime,
		FsKey:          "",
		FsType:         MDFS,
		FsShard:        ctx.fsShard,
		DomainShardId:  0,
		FileSize:       ctx.size,
		OriginFileName: ctx.filename,
		Md5:            "",
		CreationTime:   time.Now().Unix(),
		UploadID:       uploadID,
		IsUploading:    1,
	}
	if _, err = config.ResourceMap[dbIdx].Put(&ctx.resource); err != nil {
		return err
	}

	for {
		// limit read file upload to weed
		ctx.objectPart.NeedleId++
		reader, err := hash.NewReader(io.LimitReader(hashReader, int64(config.PartSize)), "")
		if err != nil {
			return err
		}

		if err = uploadObjectPart(ctx, reader, lifeCycle, fsCli, dbIdx); err != nil {
			return err
		}

		// It's best to use reader.Size() not config.PartSize
		// http post method store file in disk if file is to large, every part not last reader.Size() == config.PartSize.
		// http put method read at stream, will happen not the last part reader.Size() != config.PartSize.
		if config.PartSize*uint64(ctx.objectPart.NeedleId) >= uint64(ctx.size) {
			break
		}
	}

	ctx.md5 = hashReader.MD5HexString()
	ctx.createTime = time.Now().Unix()
	ctx.resource.Md5 = ctx.md5
	ctx.resource.CreationTime = ctx.createTime

	_, err = config.ResourceMap[dbIdx].CompleteNeedleObject(&ctx.resource)
	return err
}

func uploadObjectPart(ctx *Context, hashReader *hash.Reader, lifeCycle string, fsCli fs.Dfs, dbIdx int) (err error) {
	ctx.objectPart.FsKey, _, err = fsCli.Put(ctx.filename, ctx.mime, hashReader, GenerateArgs(lifeCycle))
	if err != nil {
		ctx.result = &Result{FileSystemError, "upload file error: " + err.Error()}
		return err
	}

	// write metadata
	ctx.objectPart.FileSize = hashReader.Size()
	ctx.objectPart.Md5 = hashReader.MD5HexString()
	ctx.objectPart.CreationTimestamp = time.Now().Unix()
	ctx.objectPart.ModifyTimestamp = time.Now().Unix()
	_, err = config.ObjectPartMap[dbIdx].PutObjectPart(&ctx.objectPart)
	return err
}

func isEnableSegment(ns string) bool { return config.RequireConfigData().Namespace[ns].SegmentEnable }

func isSmallObject(size int64) bool { return size <= int64(config.PartSize) }

func isLargeObject(size int64) bool { return size > int64(config.PartSize) }

func IsBadDigestError(err error) bool {
	_, ok := err.(hash.BadDigest)
	if ok {
		return true
	}

	return strings.Contains(err.Error(), "Bad digest")
}

func deleteAllResource(resources []*model.Resource, fsCli fs.Dfs,  dbIdx int) error {
	for _, resource := range resources {
		if resource == nil {
			continue
		}
		switch resource.FsType {
		case DFS:
			if err := deleteDfsResource(resource, fsCli, dbIdx); err != nil {
				return err
			}
		case MDFS:
			if err := deleteMdfsResource(resource, fsCli, dbIdx); err != nil {
				return err
			}
		}
	}

	return nil
}

func deleteDfsResource(resource *model.Resource, fsCli fs.Dfs, dbIdx int) (err error) {
	// delete metadata
	if _, err := config.ResourceMap[dbIdx].Delete(resource.Ns, resource.Key, resource.Spec); err != nil {
		return err
	}

	return fsCli.Delete(resource.FsKey)
}

func deleteMdfsResource(resource *model.Resource, fsCli fs.Dfs, dbIdx int) error {
	// delete part meta and data
	objparts, err := config.ObjectPartMap[dbIdx].GetObjectParts(resource.Ns, resource.Key, resource.UploadID)
	if err != nil {
		return err
	}

	if _, err := config.ResourceMap[dbIdx].Delete(resource.Ns, resource.Key, resource.Spec); err != nil {
		return err
	}

	// ignore delete object part error
	config.ObjectPartMap[dbIdx].DeleteObjectPartsPhysics(resource.Ns, resource.Key)

	for _, objpart := range objparts {
		fsCli.Delete(objpart.FsKey)
	}

	return nil
}
