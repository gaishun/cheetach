# cheetah
cheetach (include gw)
## Quick Start
Start cheetah easily in docker by steps following:
 1. Make a device by `dd if=/dev/zero of=data bs=512 count=1 seek=10000 && sudo losetup  loop31 ./data ` ;
     or a real device like : `/dev/sdb` . We call this device as BDS(Block Data Storage) here.
 2. `docker pull jiajiashun/cheetah:latest` 
 3. `docker run -it --name cheetah --device ${BDS}:/dev/sdv  jiajiashun/cheetah:latest /bin/bash`  to start a container
     a. in container : `cd /home/dss/src/main && bash build.sh && ./main1` to start a Data Storage Sevice
 4. `docker execi -it cheetah /bin/bash` to entry the container started just now.
      a. in the same container: ` cd /home/mds/src/mdsgrpc && bash build.sh && ./gsmds1` to start a Metadata Service
 5. `docker execi -it cheetah /bin/bash` to entry the container started just now.
       a. in the same container: ` cd /home/gw/src/gproxy && bash build.sh && ./gproxy` to start a goproxy.
 
