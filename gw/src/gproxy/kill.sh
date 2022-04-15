ps -ef | grep gproxy | grep -v grep | awk '{print $2}' | xargs kill -9
