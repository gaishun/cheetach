package model

import (
	"database/sql"
	"errors"
	"fmt"
	"hash/crc32"
	_ "lib/mysql"
	"strings"
	"time"

	"pkg/retry"
)

const (
	Columns    = "namespace,resource_key,spec,mime,fs_key,fs_type,fs_shard_id,domain_shard_id,file_size,md5,origin_file_name,creation_timestamp,upload_id,is_uploading"
	UpdateMeta = "mime=?,fs_key=?,fs_shard_id=?,domain_shard_id=?,file_size=?,md5=?,origin_file_name=?,creation_timestamp=?"
)

type Resource struct {
	Ns             string
	Key            string
	Spec           string
	Mime           string
	FsKey          string
	FsType         string
	FsShard        int
	DomainShardId  int
	FileSize       int64
	OriginFileName string
	Md5            string
	CreationTime   int64
	UploadID       string
	IsUploading    int
}

func NewResource(rows *sql.Rows) (res *Resource, err error) {
	res = new(Resource)
	err = rows.Scan(&res.Ns,
		&res.Key,
		&res.Spec,
		&res.Mime,
		&res.FsKey,
		&res.FsType,
		&res.FsShard,
		&res.DomainShardId,
		&res.FileSize,
		&res.Md5,
		&res.OriginFileName,
		&res.CreationTime,
		&res.UploadID,
		&res.IsUploading,
	)

	return res, err
}

func insertArg(resource *Resource) (value string, args []interface{}) {
	cnt := len(strings.Split(Columns, ","))
	valueArray := make([]string, cnt, cnt)
	for i := 0; i < cnt; i++ {
		valueArray[i] = "?"
	}
	value = "(" + strings.Join(valueArray, ",") + ")"
	args = []interface{}{
		resource.Ns,
		resource.Key,
		resource.Spec,
		resource.Mime,
		resource.FsKey,
		resource.FsType,
		resource.FsShard,
		resource.DomainShardId,
		resource.FileSize,
		resource.Md5,
		resource.OriginFileName,
		time.Now().Unix(),
		resource.UploadID,
		resource.IsUploading,
	}

	return value, args
}

func insertArgs(resource []*Resource) (value string, args []interface{}) {
	valueArray := []string{}
	args = []interface{}{}

	for _, res := range resource {
		value, arg := insertArg(res)
		valueArray = append(valueArray, value)
		args = append(args, arg)
	}

	return strings.Join(valueArray, ","), args
}

func updateArg(resource *Resource) (args []interface{}) {
	args = []interface{}{
		resource.Mime,
		resource.FsKey,
		resource.FsShard,
		resource.DomainShardId,
		resource.FileSize,
		resource.Md5,
		resource.OriginFileName,
		time.Now().Unix(),
		resource.Ns,
		resource.Key,
		resource.Spec,
	}
	return
}

func NewResourceMap(addr string, maxConn int, maxIdle int) (ResourceMapper, error) {
	db, err := sql.Open("mysql", addr)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(maxIdle)
	db.SetMaxOpenConns(maxConn)
	if err = db.Ping(); err != nil {
		return nil, errors.New("ping db fail:" + err.Error())
	}

	return &ResourceMap{
		db:           db,
		shardingFunc: DefaultSharding,
	}, nil
}

func (resmap *ResourceMap) getTable(ns string, key string) string {
	shard := resmap.shardingFunc(ns, key)
	table := fmt.Sprintf("resource_%d", shard)
	return table
}

func (resmap *ResourceMap) Get(ns, key, spec string) (resource *Resource, err error) {
	var rows *sql.Rows
	table := resmap.getTable(ns, key)
	sql := fmt.Sprintf("select %s from %s where namespace=? and resource_key=? and spec=?", Columns, table)
	rows, err = resmap.db.Query(sql, ns, key, spec)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	resources := []*Resource{}
	for rows.Next() {
		var res *Resource
		res, err = NewResource(rows)
		if err != nil {
			return nil, err
		}
		resources = append(resources, res)
	}
	if len(resources) > 0 {
		return resources[len(resources)-1], nil
	}
	return nil, errors.New("reosurce not exit")
}

func (resmap *ResourceMap) Gets(ns, key string) (resources []*Resource, err error) {
	var rows *sql.Rows
	table := resmap.getTable(ns, key)
	sql := fmt.Sprintf("select %s from %s where namespace=? and resource_key=?", Columns, table)
	rows, err = resmap.db.Query(sql, ns, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	resources = []*Resource{}
	for rows.Next() {
		var res *Resource
		res, err = NewResource(rows)
		if err != nil {
			return resources, err
		}
		resources = append(resources, res)
	}

	return resources, nil
}

func (resmap *ResourceMap) Scan(table, ns string, recordId int64, limit uint) (resources []*Resource, err error) {
	var rows *sql.Rows
	sql := fmt.Sprintf("select %s from %s where id>=? and namespace=? limit ?", Columns, table)
	rows, err = resmap.db.Query(sql, recordId, ns, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	resources = []*Resource{}
	for rows.Next() {
		var res *Resource
		res, err = NewResource(rows)
		if err != nil {
			return resources, err
		}
		resources = append(resources, res)
	}
	return resources, nil
}

func (resmap *ResourceMap) Put(resource *Resource, options ...retry.RetryOption) (affects int, err error) {
	err = retry.Retry(func() error {
		affects, err = resmap.put(resource)
		return err
	})
	return affects, err
}

func (resmap *ResourceMap) put(resource *Resource) (int, error) {
	table := resmap.getTable(resource.Ns, resource.Key)
	value, args := insertArg(resource)
	sql := fmt.Sprintf("insert into %s(%s) values %s", table, Columns, value)
	result, err := resmap.db.Exec(sql, args...)
	if err != nil {
		return 0, err
	}
	cnt, err := result.RowsAffected()

	return int(cnt), err
}

func (resmap *ResourceMap) Puts(resources []*Resource) (int, error) {
	table := resmap.getTable(resources[0].Ns, resources[0].Key)
	values, args := insertArgs(resources)
	sql := fmt.Sprintf("insert into %s(%s) values %s", table, Columns, values)
	result, err := resmap.db.Exec(sql, args...)
	if err != nil {
		return 0, err
	}
	cnt, err := result.RowsAffected()

	return int(cnt), err
}

func (resmap *ResourceMap) Update(resource *Resource) (int, error) {
	table := resmap.getTable(resource.Ns, resource.Key)
	args := updateArg(resource)
	sql := fmt.Sprintf("update %s set %s where namespace=? and resource_key=? and spec=?", table, UpdateMeta)
	result, err := resmap.db.Exec(sql, args...)
	if err != nil {
		return 0, err
	}
	cnt, err := result.RowsAffected()

	return int(cnt), err
}

func (resmap *ResourceMap) CompleteNeedleObject(resource *Resource) (affects int, err error) {
	err = retry.Retry(func() error {
		affects, err = resmap.completeNeedleObject(resource)
		return err
	})
	return affects, err
}

func (resmap *ResourceMap) completeNeedleObject(resource *Resource) (int, error) {
	table := resmap.getTable(resource.Ns, resource.Key)
	sql := fmt.Sprintf("update %s set md5=?, is_uploading=? where namespace=? and resource_key=? and upload_id=? and is_uploading=1", table)
	result, err := resmap.db.Exec(sql, resource.Md5, 0, resource.Ns, resource.Key, resource.UploadID)
	if err != nil {
		return 0, err
	}

	cnt, err := result.RowsAffected()
	return int(cnt), err
}

func (resmap *ResourceMap) Delete(ns, key, spec string, options ...retry.RetryOption) (affects int, err error) {
	err = retry.Retry(func() error {
		affects, err = resmap.delete(ns, key, spec)
		return err
	})
	return affects, err
}

func (resmap *ResourceMap) delete(ns, key, spec string) (int, error) {
	table := resmap.getTable(ns, key)
	sql := fmt.Sprintf("delete from %s where namespace=? and resource_key=? and spec=?", table)
	result, err := resmap.db.Exec(sql, ns, key, spec)
	if err != nil {
		return 0, err
	}
	cnt, err := result.RowsAffected()

	return int(cnt), err
}

func (resmap *ResourceMap) Deletes(ns, key string) (int, error) {
	table := resmap.getTable(ns, key)
	sql := fmt.Sprintf("delete from %s where namespace=? and resource_key=?", table)
	result, err := resmap.db.Exec(sql, ns, key)
	if err != nil {
		return 0, err
	}
	cnt, err := result.RowsAffected()

	return int(cnt), err
}

func (resmap *ResourceMap) Close() {
	resmap.db.Close()
}

type ShardingFunc func(ns string, key string) uint32

const ShardingCount = 200

func DefaultSharding(ns string, key string) uint32 {
	key = fmt.Sprintf("%s#%s", ns, key)
	hashValue := crc32.ChecksumIEEE([]byte(key))
	return hashValue % ShardingCount
}

const (
	SCAN_LIMIT      = 2000
	SCAN_SLEEP_TIME = 20
	EXPIRE          = 30 * 24 * 3600
)

func ClearDaemon(imgdb ResourceMapper) {
	clear := func() {
		for i := 0; i < ShardingCount; i++ {
			table := fmt.Sprintf("resource_%d", i)
			recordId := int64(0)
			for {
				resources, err := imgdb.Scan(table, "imgcache", recordId, SCAN_LIMIT)
				if err != nil {
					return
				}
				for _, resource := range resources {
					if time.Now().Unix()-resource.CreationTime >= EXPIRE {
						_, err := imgdb.Delete(resource.Ns, resource.Key, resource.Spec)
						if err != nil {
							break
						}
					}
				}
				if len(resources) < SCAN_LIMIT {
					break
				}
				recordId += int64(len(resources))
				time.Sleep(SCAN_SLEEP_TIME * time.Millisecond)
			}
		}
	}

	// 定时器、每晚12点开始执行
	for {
		clear()
		now := time.Now()
		// 计算下一个零点
		next := now.Add(time.Hour * 24)
		next = time.Date(next.Year(), next.Month(), next.Day(), 0, 0, 0, 0, next.Location())
		t := time.NewTimer(next.Sub(now))
		<-t.C
	}
}
