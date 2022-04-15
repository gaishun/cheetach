mv ../service-direct/grpc_put.go.chuanxing ../service-direct/grpc_put.go
bash build.sh
echo "BUILD COMPLETE"
mv ../service-direct/grpc_put.go ../service-direct/grpc_put.go.chuanxing
bash kill.sh
echo "KILLED"
sleep 1
bash start.sh
echo "STARTED"
tail nohup.out
