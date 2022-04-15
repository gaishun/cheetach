#!/bin/bash

bash kill.sh
sleep 1
bash start.sh
tail -fn 50 nohup.out
