
rm -rf /disk1/luoxinyuan-rocksdb/*
rm -f nohup.out
nohup ./gsmds1 &
sleep 1
netstat -ntlp
tail -fn 50 nohup.out
