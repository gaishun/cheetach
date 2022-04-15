package service_direct

import (
	"bytes"
	"context"
	"fmt"
	"../config"
	mds "./mdspb"
	"log"
	"time"
)

func DeleteFileLogicGrpc(ctx *Context) {
	buf := bytes.NewBuffer(nil)
	buf.WriteString("ng-start-read: ")
	mdsAddr := SelectMds(ctx.ns, ctx.key)
	if mdsAddr == "" {
		ctx.result = &Result{InternalError, fmt.Sprintf("no mds to use")}
		return
	}

	// conn, err := grpc.Dial(mdsAddr, DialOpt...)
	conn, err := config.MDSPool[mdsAddr].Get()
	if err != nil {
		ctx.result = &Result{InternalError, fmt.Sprintf("can't connect: %v", err)}
		return
	}
	defer conn.Close()

	client := mds.NewMetadataServerClient(conn.Value())
	req := mds.SpaceRequest{
		Name: ctx.ns + ctx.key,
		Size: uint64(ctx.size),
	}

	resp, err := client.RemoveMetadata(context.Background(), &req)
	buf.WriteString(fmt.Sprintf("MDS返回点1:%v ",time.Since(ctx.start).Nanoseconds()))
	if err != nil {
		ctx.result = &Result{InternalError, fmt.Sprintf("remove space error: %v", err)}
		return
	}

	if resp.Ret < 0 {
		ctx.result = &Result{InternalError, fmt.Sprintf("remove space ret error: %d", resp.Ret)}
		return
	}
	log.Println(buf.String())

	return
}
