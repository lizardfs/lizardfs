CHUNKSERVERS=1 \
	MOUNTS=2 \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"

touch file
touch file2
touch file3

ln -s file symlink
assert_success ls -l | grep "symlink -> file"
rm symlink
ln -s file2 symlink2
assert_success ls -l | grep "symlink2 -> file2"
rm symlink2

cd "${info[mount1]}"

ln -s file3 symlink3

cd "${info[mount0]}"

assert_success ls -l | grep "symlink3 -> file3"
