rm nohup.out
nohup  ./main1 &
mv  nohup.out nohup.out
ps -ef | grep ./main
netstat -ntlp | grep ./main
