timeout_set 1 minute

CHUNKSERVERS=1 \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

# FILE_COUNT has to be greater than ~128 to effectively test readdir
FILE_COUNT=200
MASTER_CFG_FILE="${info[master${info[current_master]}_cfg]}"

cd "${info[mount0]}"
touch $(seq 1 $FILE_COUNT)

FILES_ITERATED=$(python3 - <<END_OF_SCRIPT
import os, itertools
slice_size = int(${FILE_COUNT}) // 2
dirents = 0
with os.scandir("${info[mount0]}") as dir:
	dirents += len(list(itertools.islice(dir, slice_size)))
	os.system("mfsmaster -c ${MASTER_CFG_FILE} restart")
	dirents += len(list(itertools.islice(dir, slice_size)))
print(dirents)
END_OF_SCRIPT
)

assert_equals $FILE_COUNT $FILES_ITERATED
