package model

import (
	"database/sql"
	"errors"
	"fmt"
	"pkg/retry"
	"strings"
	"time"
)

var ErrObjectPartNotExist = errors.New("object part not exit")

type ObjectPart struct {
	BucketName        string
	ObjectKey         string
	UploadId          string
	PartId            int
	NeedleId          int
	FsKey             string
	FsShard           int
	FsType            string
	IsDeleted         int
	FileTTL           int64
	FileSize          int64
	Md5               string
	Version           string
	CreationTimestamp int64
	ModifyTimestamp   int64
}

const RetryNumber = 3
const LimitNumber = 100

const (
	ObjectPartColumns = "bucket_name,object_key,upload_id,part_id,needle_id,fs_key,fs_shard_id,fs_type,is_deleted,file_ttl,file_size,md5,version,creation_timestamp,modify_timestamp"
)

func NewObjectPartMap(addr string, maxConn, maxIdle int, oneTab bool) (ObjectPartMapper, error) {
	db, err := sql.Open("mysql", addr)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(maxIdle)
	db.SetMaxOpenConns(maxConn)
	if err = db.Ping(); err != nil {
		return nil, errors.New("ping db fail:" + err.Error())
	}

	return &ObjectPartMap{
		oneTab:       oneTab,
		db:           db,
		shardingFunc: DefaultSharding,
	}, nil
}

func (o *ObjectPartMap) getAllTables() []string {
	if o.oneTab {
		return []string{"object_part"}
	}

	result := make([]string, ShardingCount)
	for i := 0; i < ShardingCount; i++ {
		result[i] = fmt.Sprintf("object_part_%d", i)
	}
	return result
}

func (o *ObjectPartMap) getTable(bucket, object string) string {
	if o.oneTab == true {
		return "object_part"
	} else {
		shard := o.shardingFunc(bucket, object)
		table := fmt.Sprintf("object_part_%d", shard)
		return table
	}
}

func (o *ObjectPartMap) GetObjectPart(bucket, object, uploadId string, partId int) (*ObjectPart, error) {
	var rows *sql.Rows
	table := o.getTable(bucket, object)
	sql := fmt.Sprintf("select %s from %s where bucket_name=? and object_key=? and upload_id=? and part_id=? and is_deleted=0", ObjectPartColumns, table)
	rows, err := o.db.Query(sql, bucket, object, uploadId, partId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	objectParts := []*ObjectPart{}
	for rows.Next() {
		var res *ObjectPart
		res, err = newObjectPart(rows)
		if err != nil {
			return nil, err
		}
		objectParts = append(objectParts, res)
	}
	if len(objectParts) > 0 {
		return objectParts[0], nil
	}
	return nil, ErrObjectPartNotExist
}

func (o *ObjectPartMap) GetDeletedObjectPart(bucket, object, uploadId string, partId int) ([]*ObjectPart, error) {
	var rows *sql.Rows
	table := o.getTable(bucket, object)
	sql := fmt.Sprintf("select %s from %s where bucket_name=? and object_key=? and upload_id=? and part_id=? and is_deleted=1", ObjectPartColumns, table)
	rows, err := o.db.Query(sql, bucket, object, uploadId, partId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	objectParts := []*ObjectPart{}
	for rows.Next() {
		var res *ObjectPart
		res, err = newObjectPart(rows)
		if err != nil {
			return nil, err
		}
		objectParts = append(objectParts, res)
	}
	return objectParts, nil
}

func newObjectPart(rows *sql.Rows) (*ObjectPart, error) {
	objectPart := new(ObjectPart)
	err := rows.Scan(
		&objectPart.BucketName,
		&objectPart.ObjectKey,
		&objectPart.UploadId,
		&objectPart.PartId,
		&objectPart.NeedleId,
		&objectPart.FsKey,
		&objectPart.FsShard,
		&objectPart.FsType,
		&objectPart.IsDeleted,
		&objectPart.FileTTL,
		&objectPart.FileSize,
		&objectPart.Md5,
		&objectPart.Version,
		&objectPart.CreationTimestamp,
		&objectPart.ModifyTimestamp,
	)

	return objectPart, err
}

func (o *ObjectPartMap) PutObjectPart(objectPart *ObjectPart, options ...retry.RetryOption) (affects int, err error) {
	err = retry.Retry(func() error {
		affects, err = o.putObjectPart(objectPart)
		return err
	})
	return affects, err
}

func (o *ObjectPartMap) putObjectPart(objectPart *ObjectPart) (int, error) {
	table := o.getTable(objectPart.BucketName, objectPart.ObjectKey)
	values, args := insertObjectPartArg(objectPart)
	sql := fmt.Sprintf("insert into %s(%s) values %s", table, ObjectPartColumns, values)
	result, err := o.db.Exec(sql, args...)
	if err != nil {
		return 0, err
	}

	cnt, err := result.RowsAffected()
	return int(cnt), err
}

func insertObjectPartArg(objectPart *ObjectPart) (value string, args []interface{}) {
	cnt := len(strings.Split(ObjectPartColumns, ","))
	valueArray := make([]string, cnt, cnt)
	for i := 0; i < cnt; i++ {
		valueArray[i] = "?"
	}
	value = "(" + strings.Join(valueArray, ",") + ")"
	args = []interface{}{
		objectPart.BucketName,
		objectPart.ObjectKey,
		objectPart.UploadId,
		objectPart.PartId,
		objectPart.NeedleId,
		objectPart.FsKey,
		objectPart.FsShard,
		objectPart.FsType,
		objectPart.IsDeleted,
		objectPart.FileTTL,
		objectPart.FileSize,
		objectPart.Md5,
		objectPart.Version,
		objectPart.CreationTimestamp,
		objectPart.ModifyTimestamp,
	}

	return value, args
}

func (o *ObjectPartMap) GetOBjectPartsV2(bucket, object, uploadId string, maxParts, partNumberMarker int) ([]*ObjectPart, error) {
	var rows *sql.Rows
	table := o.getTable(bucket, object)
	var conStr string = "where bucket_name=? and object_key=? and upload_id=?"
	andMarker := ""
	if partNumberMarker > 0 {
		andMarker = fmt.Sprintf(" and part_id > %d ", partNumberMarker)
	}
	conStr = fmt.Sprintf(" %s %s order by part_id limit %d ", conStr, andMarker, maxParts)

	sql := fmt.Sprintf("select %s from %s %s", ObjectPartColumns, table, conStr)
	rows, err := o.db.Query(sql, bucket, object, uploadId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	objectParts := []*ObjectPart{}
	for rows.Next() {
		var res *ObjectPart
		res, err = newObjectPart(rows)
		if err != nil {
			return nil, err
		}
		objectParts = append(objectParts, res)
	}

	return objectParts, nil
}

func (o *ObjectPartMap) GetObjectParts(bucket, object, uploadId string) ([]*ObjectPart, error) {
	var rows *sql.Rows
	table := o.getTable(bucket, object)
	sql := fmt.Sprintf("select %s from %s where bucket_name=? and object_key=? and upload_id=? and is_deleted=0 order by part_id,needle_id", ObjectPartColumns, table)
	rows, err := o.db.Query(sql, bucket, object, uploadId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	objectParts := []*ObjectPart{}
	for rows.Next() {
		var res *ObjectPart
		res, err = newObjectPart(rows)
		if err != nil {
			return nil, err
		}
		objectParts = append(objectParts, res)
	}

	return objectParts, nil
}

func (o *ObjectPartMap) DeleteObjectPart(bucket, object, uploadId string, partId int) (int, error) {
	table := o.getTable(bucket, object)

	sql := fmt.Sprintf("update %s set is_deleted=1, modify_timestamp=? where bucket_name=? and object_key=? and upload_id=? and part_id=? and is_deleted=0", table)

	result, err := o.db.Exec(sql, time.Now().Unix(), bucket, object, uploadId, partId)
	if err != nil {
		return 0, err
	}
	cnt, err := result.RowsAffected()

	return int(cnt), nil
}

func (o *ObjectPartMap) DeleteObjectPartPhysics(bucket, object, uploadId string, partId int, deleted int) (int, error) {
	table := o.getTable(bucket, object)

	sql := fmt.Sprintf("delete from %s where bucket_name=? and object_key=? and upload_id=? and part_id=? and is_deleted=?", table)

	result, err := o.db.Exec(sql, time.Now().Unix(), bucket, object, uploadId, partId, deleted)
	if err != nil {
		return 0, err
	}
	cnt, err := result.RowsAffected()

	return int(cnt), nil
}

func (o *ObjectPartMap) DeleteObjectPartByUploadId(bucket, object, uploadId string) (int, error) {
	table := o.getTable(bucket, object)

	sql := fmt.Sprintf("update %s set is_deleted=1, modify_timestamp=? where bucket_name=? and object_key=? and upload_id=? and is_deleted=0", table)

	result, err := o.db.Exec(sql, time.Now().Unix(), bucket, object, uploadId)
	if err != nil {
		return 0, err
	}
	cnt, err := result.RowsAffected()

	return int(cnt), nil
}

func (o *ObjectPartMap) DeleteObjectPartByUploadIdPhysics(bucket, object, uploadId string, options ...retry.RetryOption) (affects int, err error) {
	err = retry.Retry(func() error {
		affects, err = o.deleteObjectPartByUploadIdPhysics(bucket, object, uploadId)
		return err
	})
	return affects, err
}

func (o *ObjectPartMap) deleteObjectPartByUploadIdPhysics(bucket, object, uploadId string) (int, error) {
	table := o.getTable(bucket, object)

	sql := fmt.Sprintf("delete from %s where bucket_name=? and object_key=? and upload_id=? and is_deleted=0", table)

	result, err := o.db.Exec(sql, bucket, object, uploadId)
	if err != nil {
		return 0, err
	}
	cnt, err := result.RowsAffected()

	return int(cnt), nil
}

func (o *ObjectPartMap) DeleteObjectPartsPhysics(bucket, object string, options ...retry.RetryOption) (affects int, err error) {
	err = retry.Retry(func() error {
		affects, err = o.deleteObjectPartsPhysics(bucket, object)
		return err
	})
	return affects, err
}

func (o *ObjectPartMap) deleteObjectPartsPhysics(bucket, object string, options ...retry.RetryOption) (int, error) {
	table := o.getTable(bucket, object)

	sql := fmt.Sprintf("delete from %s where bucket_name=? and object_key=? and is_deleted=0", table)

	result, err := o.db.Exec(sql, bucket, object)
	if err != nil {
		return 0, err
	}
	cnt, err := result.RowsAffected()

	return int(cnt), nil
}

func (o *ObjectPartMap) ClearObjectParts(bucket string) error {
	tables := o.getAllTables()
	for _, tab := range tables {
		retry := RetryNumber
		for {
			if retry <= 0 {
				return errors.New("retry <= 0")
			}
			sql := fmt.Sprintf("update %s set is_deleted=1, modify_timestamp=? where bucket_name=? and is_deleted = 0 limit %d", tab, LimitNumber)
			result, err := o.db.Exec(sql, time.Now().Unix(), bucket)
			if err != nil {
				retry--
			}
			count, err := result.RowsAffected()
			if err != nil {
				retry--
			} else {
				if count <= 0 {
					break
				}
			}
		}
	}

	return nil
}

func (o *ObjectPartMap) CountSize(bucket, object, uploadId string) (size int64, err error) {
	tables := o.getAllTables()
	var num int64
	for _, tab := range tables {
		s := fmt.Sprintf("select sum(file_size) from %s where bucket_name=? and object_key=? and upload_id=? and is_deleted=0", tab)
		rows, err := o.db.Query(s, bucket, object, uploadId)
		if err != nil {
			return 0, err
		}
		defer rows.Close()

		for rows.Next() {
			rows.Scan(&num)
		}

		size += num
	}
	return size, nil
}

func (o *ObjectPartMap) Close() {
	o.db.Close()
}
