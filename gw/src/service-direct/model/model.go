package model

import (
	"database/sql"
	"pkg/retry"
)

type ResourceMap struct {
	db           *sql.DB
	shardingFunc ShardingFunc
}

type ResourceMapper interface {
	Get(ns, key, spec string) (resource *Resource, err error)
	Gets(ns, key string) (resources []*Resource, err error)
	Scan(table, ns string, recordId int64, limit uint) (resources []*Resource, err error)
	Put(resource *Resource, options ...retry.RetryOption) (int, error)
	Puts(resource []*Resource) (int, error)
	Update(resource *Resource) (int, error)
	CompleteNeedleObject(resource *Resource) (int, error)
	Delete(ns, key, spec string, options ...retry.RetryOption) (deleted int, err error)
	Deletes(ns, key string) (deleted int, err error)
	Close()
}

type ObjectPartMap struct {
	oneTab       bool
	db           *sql.DB
	shardingFunc ShardingFunc
}

type ObjectPartMapper interface {
	GetObjectPart(string, string, string, int) (*ObjectPart, error)
	GetDeletedObjectPart(string, string, string, int) ([]*ObjectPart, error)
	PutObjectPart(objpart *ObjectPart, options ...retry.RetryOption) (int, error)
	GetObjectParts(bucket, object, uploadId string) ([]*ObjectPart, error)
	GetOBjectPartsV2(bucket, object, uploadId string, maxParts, partNumberMarker int) ([]*ObjectPart, error)
	DeleteObjectPart(string, string, string, int) (int, error)
	DeleteObjectPartPhysics(string, string, string, int, int) (int, error)
	DeleteObjectPartByUploadId(string, string, string) (int, error)
	DeleteObjectPartByUploadIdPhysics(bucket, object, uploadId string, options ...retry.RetryOption) (int, error)
	DeleteObjectPartsPhysics(bucket, object string, options ...retry.RetryOption) (int, error)
	ClearObjectParts(string) error
	CountSize(string, string, string) (int64, error)
	Close()
}
