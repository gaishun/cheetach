package service_direct

import (
	"lib/metric"
	"strconv"
)

type OdinMetric func(caller string, ctx *Context)

/*
 * name :			MetricDownload
 * Description :	download odin统计
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-08-14
 * correcter :
 * correct date :
 * reason :
 * version :	2.002
 */
func MetricDownload(caller string, ctx *Context) {
	m := metric.NewMetric(caller)
	m.AddTag("namespace", ctx.ns)
	m.AddCounter("request", 1)
	m.AddTag("statuscode", strconv.Itoa(ctx.result.ErrorCode.StatusCode))
	m.AddTime("processTime", ctx.processTime)
	m.AddMetric("bytes", ctx.size)
	m.Emit()
}

/*
 * name :			MetricQuery
 * Description :	query odin统计
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-08-14
 * correcter :
 * correct date :
 * reason :
 * version :	2.002
 */
func MetricQuery(caller string, ctx *Context) {
	m := metric.NewMetric(caller)
	m.AddTag("namespace", ctx.ns)
	m.AddCounter("request", 1)
	m.AddTag("statuscode", strconv.Itoa(ctx.result.ErrorCode.StatusCode))
	m.AddTime("processTime", ctx.processTime)
	m.Emit()
}

/*
 * name :			MetricUpload
 * Description :	upload odin统计
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-08-14
 * correcter :
 * correct date :
 * reason :
 * version :	2.002
 */
func MetricUpload(caller string, ctx *Context) {
	m := metric.NewMetric(caller)
	m.AddTag("namespace", ctx.ns)
	m.AddCounter("request", 1)
	m.AddTag("statuscode", strconv.Itoa(ctx.result.ErrorCode.StatusCode))
	m.AddTime("processTime", ctx.processTime)
	m.AddMetric("inbytes", ctx.size)
	m.Emit()
}

/*
 * name :			MetricFskey
 * Description :	查询fsKey odin统计
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-09-03
 * correcter :
 * correct date :
 * reason :
 * version :	2.0.1.1
 */
func MetricFskey(caller string, ctx *Context) {

}

/*
 * name :			MetricBatch
 * Description :	批量查询 odin统计
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-09-03
 * correcter :
 * correct date :
 * reason :
 * version :	2.0.1.1
 */
func MetricBatch(caller string, ctx *Context) {

}

/*
 * name :			MetricImage
 * Description :	图片处理 odin统计
 * input :
 * output :
 * return :
 * creator :		shimingya
 * creat date :		2017-09-03
 * correcter :
 * correct date :
 * reason :
 * version :	2.0.1.1
 */
func MetricImage(caller string, ctx *Context) {

}
