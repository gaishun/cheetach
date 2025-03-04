package service_direct

import (
	"bytes"
	"../config"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"lib/glog"
	"lib/pool"
	pool2 "lib/pool"
	"log"
	"math/rand"
	"net/http"
	bitstore2 "./dsspb"
	mds2 "./mdspb"
	"time"
	bitstore "./dsspb"
	mds "./mdspb"
)

func UploadFileLogicGrpc(ctx *Context) {
	buf := bytes.NewBuffer(nil)
	buf.WriteString("ng-start: ")
	UploadCheck(ctx)
	if ctx.result.ErrorCode.StatusCode != http.StatusOK {
		return
	}
	bufData, err := ioutil.ReadAll(ctx.reader)
        if err != nil {
                glog.Errorf("read file error: %v", err)
                ctx.result = &Result{InternalError, fmt.Sprintf("read file error: %v",     err)}
        }
	resp, mdsConn, mdsaddr := allocMetadata(buf, ctx, len(bufData))
	if mdsConn != nil {
		defer mdsConn.Close()
	}
	if resp == nil {
		return
	}
	ctx.dssStart = time.Now()
	defer func() {
		ctx.dssTime = time.Since(ctx.dssStart).Nanoseconds() / time.Millisecond.Nanoseconds()
	}()
	conns, streams := getConns(ctx, int(resp.Ds),mdsaddr)
	defer releaseConns(conns)
	if streams == nil {
		ctx.result = &Result{InternalError, fmt.Sprintf("grcp connect stream create error")}
		return
	}
	/*bufData, err := ioutil.ReadAll(ctx.reader)
	if err != nil {
		glog.Errorf("read file error: %v", err)
		ctx.result = &Result{InternalError, fmt.Sprintf("read file error: %v", err)}
	}*/

	// concurrence send write request
	for i := 0; i < int(resp.Count); i++ {
		copySize := resp.Extents[i].Length
		if len(bufData) < int(copySize) {
			copySize = uint64(len(bufData))
		}
		data := bufData[:copySize]

		if err != io.EOF && err != nil {
			glog.Errorf("read file error: %v", err)
			ctx.result = &Result{InternalError, fmt.Sprintf("read file error: %v", err)}
			break
		}
		ctx.size += int64(len(data))
		//buf.WriteString(fmt.Sprintf("$$%v$$",string(data)))
		//buf.WriteString(fmt.Sprintf("size is %v,",ctx.size))
		req := &bitstore.PutRequest{
			Offset: resp.Extents[i].Offset,
			Length: resp.Extents[i].Length,
			Data:   data,
		}

		for _, stream := range streams {
			go func(stream bitstore.BitStore_PutClient) {
				err := stream.Send(req)
				if err != nil {
					glog.Errorf("send file error: %v", err)
				}
				ctx.dssChan <- err
			}(stream)
		}
	}
	// wait for requests send finish
	for i := 0; i < int(resp.Count)*len(streams); i++ {
		<-ctx.dssChan
	}

	// close send request
	for _, stream := range streams {
		if err := stream.CloseSend(); err != nil {
			ctx.result = &Result{InternalError, fmt.Sprintf("close stream error: %v", err)}
			return
		}
	}

	dssRltChan := make(chan error)
	// receive replicate reply
	for _, stream := range streams {
		go func(stream bitstore.BitStore_PutClient) {
			resp, err := stream.CloseAndRecv()
			if err != nil {
				dssRltChan <- err
				return
			}
			if resp.Errcode < 0 {
				dssRltChan <- fmt.Errorf("dss resp error: %v", resp.Errcode)
				return
			}
			dssRltChan <- nil
		}(stream)
	}

	// wait for dss replicate finish
	for i := 0; i < len(streams); i++ {
		err := <-dssRltChan
		if err != nil {
			ctx.result = &Result{InternalError, err.Error()}
		}
	}
	buf.WriteString(fmt.Sprintf("DSS返回点:%v ",time.Since(ctx.start).Nanoseconds()))

	// wait for mds replicate reply. timeout 5s
	select {
	case <-time.After(5 * time.Second):
		ctx.result = &Result{InternalError, fmt.Sprintf("mds replicate timeout: %ds", 5)}
	case mdsRet := <-ctx.mdsChan:
		if mdsRet != 0 {
			ctx.result = &Result{InternalError, fmt.Sprintf("mds replicate error: %v", mdsRet)}
		}
	}

	buf.WriteString(fmt.Sprintf("mds replicate time: %v --->", time.Since(ctx.start).Nanoseconds()))

	log.Println(buf.String())
}

func releaseConns(conns []pool.Conn) {
	for _, conn := range conns {
		conn.Close()
	}
}

func getdssaddrs(diskid int, mdsaddr string) []string {
	var addrs []string
	addrs = make([]string , 0, config.DssReplicateFactor+1)
	addrs =  append(addrs ,config.Mastermds2MasterDss[mdsaddr][diskid])
	for i:=0 ; i<config.DssReplicateFactor ; i++ {
		addrs = append(addrs , config.ReplicationDss[addrs[0]][i])
	}
	return addrs
}

func getConns(ctx *Context, diskid int, mdsaddr string) ([]pool2.Conn, []bitstore2.BitStore_PutClient) {
	addrs := getdssaddrs(diskid,mdsaddr)
	conns := make([]pool.Conn, 0, len(addrs))
	streams := make([]bitstore.BitStore_PutClient, 0, len(addrs))

	for _, addr := range addrs {
		//conn, err := grpc.Dial(addr, DialOpt...)
		conn, err := config.DSSPool[addr].Get()
		if err != nil {
			ctx.result = &Result{InternalError, fmt.Sprintf("can't connect: %v", err)}
			return conns, nil
		}
		client := bitstore.NewBitStoreClient(conn.Value())
		stream, err := client.Put(context.Background())
		if err != nil {
			ctx.result = &Result{InternalError, fmt.Sprintf("put stream error: %v", err)}
			return conns, nil
		}

		conns = append(conns, conn)
		streams = append(streams, stream)
	}

	return conns, streams
}

func allocMetadata(buf *bytes.Buffer, ctx *Context, size int) (*mds2.SpaceResponse, pool2.Conn, string) {

	mdsAddr := SelectMds(ctx.ns, ctx.key)
	//log.Printf("choose mds %v", mdsAddr)
	if mdsAddr == "" {
		ctx.result = &Result{InternalError, fmt.Sprintf("no mds to use")}
		return nil, nil, ""
	}

	conn, err := config.MDSPool[mdsAddr].Get()
	if err != nil {
		ctx.result = &Result{InternalError, fmt.Sprintf("can't connect: %v", err)}
		return nil, nil, ""
	}
	client := mds.NewMetadataServerClient(conn.Value())
	req := mds.SpaceRequest{
		Name: ctx.ns + ctx.key,
		Size: uint64(size),
	}
	buf.WriteString(fmt.Sprintf("MDSCONNECT:%v ",time.Since(ctx.start).Nanoseconds()))
	stream, err := client.GetSpace(context.Background(), &req)
	if err != nil {
		ctx.result = &Result{InternalError, fmt.Sprintf("get space request error: %v", err)}
		return nil, conn, ""
	}
	sctx := stream.Context()
	buf.WriteString(fmt.Sprintf("send space req time: %v --->", time.Since(ctx.start).Nanoseconds()))
	// first relpy: allocate space from memory
	resp, err := stream.Recv()
	buf.WriteString(fmt.Sprintf("MDS返回点1:%v ",time.Since(ctx.start).Nanoseconds()))
	if err != nil {
		ctx.result = &Result{InternalError, fmt.Sprintf("get space error: %v", err)}
		return nil, conn, ""
	}

	if resp.Ret < 0 {
		ctx.result = &Result{InternalError, fmt.Sprintf("get space ret error: %d", resp.Ret)}
		return nil, conn, ""
	}

	if resp.Count == 0 {
		ctx.result = &Result{InternalError, fmt.Sprintf("no space to allocate")}
		return nil, conn, ""
	}

	// async recv secend reply: sync space to other replicate
	go func() {
		for {
			select {
			case <-sctx.Done():
				if sctx.Err() != nil {
					ctx.mdsChan <- -1
				} else {
					ctx.mdsChan <- 0
				}
				return
			default:
				resp, err := stream.Recv()
				buf.WriteString(fmt.Sprintf("MDS返回点2:%v ",time.Since(ctx.start).Nanoseconds()))
				if err != nil {
					ctx.mdsChan <- -1
				} else if resp.Ret < 0 {
					ctx.mdsChan <- -1
				} else {
					ctx.mdsChan <- 0
				}
				return
			}
		}
	}()
	return resp, conn ,mdsAddr
}

func GenerateKeyGrpc() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	candidates := []byte(Characters)
	buf := bytes.NewBuffer(nil)
	count := len(Characters)
	for i := 0; i < KeySize; i++ {
		index := r.Intn(count)
		buf.WriteByte(candidates[index])
	}
	return fmt.Sprintf("%s%s", MagicNum, buf.String())
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func RandStringBytesMaskImpr(n int) []byte {
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return b
}

