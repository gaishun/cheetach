mv ../service-direct/grpc_put.go.withoutmds ../service-direct/grpc_put.go
bash build
echo "BUILD COMPLETE"
mv ../service-direct/grpc_put.go ../service-direct/grpc_put.go.withoutmds
bash kill.sh
echo "KILLED"
sleep 1
bash start.sh
echo "STARTED"
tail nohup.out
