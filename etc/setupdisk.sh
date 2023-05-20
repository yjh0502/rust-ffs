dd if=/dev/zero of=/tmp/test.img bs=1M count=64
vnconfig vnd0 /tmp/test.img
disklabel -E vnd0
newfs vnd0a
mount /dev/vnd0a /mnt/test
cp -a /src/pfstatsd/* /mnt/test
umount /mnt/test
vnconfig -u vnd0
