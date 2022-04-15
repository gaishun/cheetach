package opera

import (
  "context"
 // "lib/glog"
  "lib/pool"
  "log"
  //"google.golang.org/grpc"
  mds "../mdspb"
)

var(
  MasterMds  string
  SlaveMds 	=  []string{}//replication
  SlavePool   map[string] pool.Pool
)

func Init_pool () {
//  MasterMds = "192.168.9.116:4306"
  SlavePool = make(map[string]pool.Pool)
  for _, slave := range SlaveMds {
    p , err := getPool(slave)
    if err != nil {
      log.Fatalf("failed to init mds %v grpc pool: %v", slave , err)
    }
    SlavePool[slave] = p
  }
}

func getPool (addr string ) (pool.Pool , error) {
  opt := pool.DefaultOptions
  opt.MaxIdle = 64
  opt.MaxActive = 64
  opt.MaxConcurrentStreams = 128
  opt.Reuse = true

  log.Printf("ready to conn mds  : %s\n", addr)
  return pool.New(addr, opt)
}


func Read (req mds.SpaceRequest) error{
  var err error
  err=nil
  for _, slave := range SlaveMds {
    temp := grpc_get(slave , req)
    if temp != nil {
      err = temp
    }
  }
  return err
}

func Write (name string, size uint64 ,ds uint32,offset int64,double_duan bool,duan_num int32,mode bool) error{
  req := mds.SpaceRequest{
    Name: name,
    Size: size,
    Ds: ds,
    Offset: offset,
    DoubleDuan: double_duan,
    DuanNum: duan_num,
    Mode: mode,
  }
  var err error
  err=nil
  for _, slave := range SlaveMds {
    temp := grpc_put(slave , req)
    if temp != nil {
      err = temp
    }
  }
  return err
}

func Delete(name string, size uint64,mode bool) error {
  req := mds.SpaceRequest{
    Name: name,
    Size: size,
    Mode: mode,
  }
  var err error
  err=nil
  for _, slave := range SlaveMds {
    temp := grpc_del(slave , req)
    if temp != nil {
      err = temp
    }
  }
  return err
}

func Assign(Duan []uint64, Count int32,mode bool) error {
  req := mds.AssignDssRequest{
    Duan: Duan,
    Count: Count,
    Mode: mode,
  }
  var err error
  err=nil
  for _, slave := range SlaveMds {
    temp := grpc_assign(slave , req)
    if temp != nil {
      err = temp
    }
  }
  return err
}


//给一个地址和一个请求，返回错误信息
//不需要读副本。
func grpc_get (addr string, req mds.SpaceRequest) error {
  conn , err := SlavePool[addr].Get()
  //conn , err := grpc.Dial(addr, grpc.WithInsecure())
  if err != nil {
    log.Printf("GET fail to connect: %v\n", err)
    return err
  }
  defer conn.Close()

  //client := mds.NewMetadataServerClient(conn)
  client := mds.NewMetadataServerClient(conn.Value())
  resp , err := client.GetMetadata(context.Background(),&req)
  if err != nil {
    log.Printf("GET fail to revoke: %v\n", err)
    return err
  }
  if resp.Count == 0 {
    log.Printf("GET fail  at slave logical: %v\n", err)
    return err
  }
  return nil
}

func grpc_put (addr string, req mds.SpaceRequest) error {
  //conn , err := grpc.Dial(addr, grpc.WithInsecure())
  conn , err := SlavePool[addr].Get()
  if err != nil {
    log.Printf("PUT fail to connect: %v\n", err)
    return err
  }
  defer conn.Close()
  client := mds.NewMetadataServerClient(conn.Value())
  //client := mds.NewMetadataServerClient(conn)

  stream, err := client.GetSpace(context.Background(), &req)
  if err != nil {
    log.Printf("PUT fail to revoke getSpace: %v\n", err)
    return err
  }
  _, err = stream.Recv()
  if err != nil {
    log.Printf("PUT fail to recieve: %v\n", err)
    return err
  }
  err = stream.CloseSend()
  if err != nil {
    log.Printf("PUT fail to close stream: %v\n", err)
    return err
  }
  return nil
}

func grpc_del (addr string, req mds.SpaceRequest) error {
//  conn , err := grpc.Dial(addr, grpc.WithInsecure())
  conn , err := SlavePool[addr].Get()
  if err != nil {
    log.Printf("DEL fail to connect: %v\n", err)
    return err
  }
  defer conn.Close()
  client := mds.NewMetadataServerClient(conn.Value())
  //client := mds.NewMetadataServerClient(conn)
  _, err = client.RemoveMetadata(context.Background(), &req)
  if err != nil {
    log.Printf("DEL fail to revoke: %v\n", err)
    return err
  }
  return nil

}

func grpc_assign (addr string, req mds.AssignDssRequest) error {
  conn , err := SlavePool[addr].Get()
  //conn , err := grpc.Dial(addr, grpc.WithInsecure())
  if err != nil {
    log.Printf("Ass fail to connect: %v\n", err)
    return err
  }
  defer conn.Close()
  client := mds.NewMetadataServerClient(conn.Value())
  //client := mds.NewMetadataServerClient(conn)
  _, err = client.AssignDss(context.Background(), &req)
  if err != nil {
    log.Printf("Ass fail to revoke: %v\n", err)
    return err
  }
  return nil
}






























