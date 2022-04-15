mkfs -t ext4 /dev/nvme0n1p1
mkfs -t ext4 /dev/nvme0n1p2
mkfs -t ext4 /dev/nvme0n1p3
mkfs -t ext4 /dev/nvme0n1p4

mkdir /mnt/nvme11
mkdir /mnt/nvme12
mkdir /mnt/nvme13
mkdir /mnt/nvme14

mount /dev/nvme0n1p1  /mnt/nvme11
mount /dev/nvme0n1p2  /mnt/nvme12
mount /dev/nvme0n1p3  /mnt/nvme13
mount /dev/nvme0n1p4  /mnt/nvme14

