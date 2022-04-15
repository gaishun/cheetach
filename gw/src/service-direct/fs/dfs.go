package fs

import (
	"../../config"
	"fmt"
	"io"
	"lib/weedo"
	"net/http"
	"net/url"
	"pkg/httputil"
	"pkg/retry"
)

type Dfs interface {
	Put(filename, mime string, reader io.Reader, args url.Values, options ...retry.RetryOption) (fskey string, size int64, err error)
	Delete(fskey string, options ...retry.RetryOption) error
	Get(header http.Header, fskey string, options ...retry.RetryOption) (*http.Response, error)
	GetUrl(fskey string, options ...retry.RetryOption) (publicUrl string, privateUrl string, err error)
}

type DfsCient struct {
	client *weedo.Client
}

// New Dfs client
func NewDfsClient(fsShard int) (Dfs, error) {
	client, err := config.NewWeedoClient(fsShard)
	return &DfsCient{client: client}, err
}

func (dfs *DfsCient) Put(filename, mime string, reader io.Reader, args url.Values, options ...retry.RetryOption) (fskey string, size int64, err error) {
	err = retry.Retry(func() error {
		fskey, size, err = dfs.put(filename, mime, reader, args)
		return err
	})
	return fskey, size, err
}

func (dfs *DfsCient) Delete(fskey string, options ...retry.RetryOption) error {
	return retry.Retry(func() error { return dfs.delete(fskey) })
}

func (dfs *DfsCient) Get(header http.Header, fskey string, options ...retry.RetryOption) (resp *http.Response, err error) {
	err = retry.Retry(func() error {
		resp, err = dfs.get(header, fskey)
		return err
	})
	return resp, err
}

func (dfs *DfsCient) GetUrl(fskey string, options ...retry.RetryOption) (publicUrl string, privateUrl string, err error) {
	err = retry.Retry(func() error {
		publicUrl, privateUrl, err = dfs.getUrl(fskey)
		return err
	})
	return publicUrl, privateUrl, err
}

func (dfs *DfsCient) put(filename, mime string, reader io.Reader, args url.Values) (fskey string, size int64, err error) {
	return dfs.client.AssignUploadArgs(filename, mime, reader, args)
}

func (dfs *DfsCient) delete(fskey string) error {
	return dfs.client.Delete(fskey, 1)
}

func (dfs *DfsCient) get(header http.Header, fskey string) (*http.Response, error) {
	publicUrl, url, err := dfs.GetUrl(fskey)
	if err != nil || publicUrl == "" || url == "" {
		return nil, fmt.Errorf("get dfs url failed: %v", err)
	}

	return httputil.Get(header, publicUrl)
}

func (dfs *DfsCient) getUrl(fskey string) (publicUrl string, privateUrl string, err error) {
	return dfs.client.GetUrl(fskey)
}
