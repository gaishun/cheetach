ps -ef | grep gsmds | grep -v grep | awk '{print $2}' | xargs kill -9
