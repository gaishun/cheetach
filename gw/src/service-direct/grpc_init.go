package service_direct

import (
	"time"

	"google.golang.org/grpc"
)

const (
	InitialWindowSize = 1024 * 1024 * 1024
	MaxMsgSize        = 1024 * 1024 * 1024
	TimeOut           = 5 * time.Second
)

var (
	CallOpt = make([]grpc.CallOption, 0)
	DialOpt = make([]grpc.DialOption, 0)
)

func init() {
	CallOpt = append(CallOpt, grpc.MaxCallSendMsgSize(MaxMsgSize))
	CallOpt = append(CallOpt, grpc.MaxCallRecvMsgSize(MaxMsgSize))
	DialOpt = append(DialOpt, grpc.WithInsecure())
	DialOpt = append(DialOpt, grpc.WithInitialWindowSize(InitialWindowSize))
	DialOpt = append(DialOpt, grpc.WithDefaultCallOptions(CallOpt...))
}
