 mv ../service-direct/grpc_put.go.bingxing ../service-direct/grpc_put.go 
 bash build.sh 
echo "BUILD COMPLETE"
 mv ../service-direct/grpc_put.go ../service-direct/grpc_put.go.bingxing 
bash kill.sh 
echo "KILLED"

sleep 1 
bash start.sh 
sleep 1 
tail nohup.out -fn 50 
