CHUNKSERVERS=4 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

if ((LIZARDFS_BLOCKS_IN_CHUNK != 1024 || LIZARDFS_BLOCK_SIZE != 65536)); then
	# TODO fix this test for different sizes
	test_end
fi

# Some constants
header_size=$((5 * 1024))
xor_header_size=$((4 * 1024))
block=$LIZARDFS_BLOCK_SIZE

# Hashmaps file -> realsize/size/length
declare -A length
declare -A size
declare -A realsize

cd "${info[mount0]}"
mkdir dir

# 100 KB, goal 2
touch dir/file1
mfssetgoal 2 dir/file1
dd if=/dev/zero of=dir/file1 bs=100KiB count=1
	  length[dir/file1]=$(parse_si_suffix 100K)
	    size[dir/file1]=$((1 * header_size + 2 * block))
	realsize[dir/file1]=$((2 * header_size + 4 * block))

# 1 B, goal 3
touch dir/file2
mfssetgoal 3 dir/file2
dd if=/dev/zero of=dir/file2 bs=1 count=1
	  length[dir/file2]=1
	    size[dir/file2]=$((1 * header_size + 1 * block))
	realsize[dir/file2]=$((3 * header_size + 3 * block))

# 64 KB, goal 2
touch dir/file3
mfssetgoal 2 dir/file3
dd if=/dev/zero of=dir/file3 bs=64KiB count=1
	length[dir/file3]=65536
	    size[dir/file3]=$((1 * header_size + 1 * block))
	realsize[dir/file3]=$((2 * header_size + 2 * block))

# 1 KB, goal xor2
touch dir/filex1
mfssetgoal xor2 dir/filex1
dd if=/dev/zero of=dir/filex1 bs=1KiB count=1
	  length[dir/filex1]=$(parse_si_suffix 1K)
	    size[dir/filex1]=$((1 * header_size + 1 * block))
	realsize[dir/filex1]=$((3 * xor_header_size + 2 * block))

# 100 KB, goal xor2
touch dir/filex2
mfssetgoal xor2 dir/filex2
dd if=/dev/zero of=dir/filex2 bs=100KiB count=1
	  length[dir/filex2]=$(parse_si_suffix 100K)
	    size[dir/filex2]=$((1 * header_size + 2 * block))
	realsize[dir/filex2]=$((3 * xor_header_size + 3 * block))

# 70 MB, goal xor3
touch dir/filex3
mfssetgoal xor3 dir/filex3
dd if=/dev/zero of=dir/filex3 bs=1MiB count=70
	  length[dir/filex3]=$(parse_si_suffix 70M)
	    size[dir/filex3]=$((2 * header_size + 1120 * block))
	realsize[dir/filex3]=$((8 * xor_header_size + (2 * 373 + 2 * 374) * block))

# 70 MB + 1 B, goal xor2
touch dir/filex4
mfssetgoal xor2 dir/filex4
dd if=/dev/zero of=dir/filex4 bs=1MiB count=70
echo >> dir/filex4
	  length[dir/filex4]=$(($(parse_si_suffix 70M) + 1))
	    size[dir/filex4]=$((2 * header_size + 1121 * block))
	realsize[dir/filex4]=$((6 * xor_header_size + (560 + 2 * 561) * block))

# 64 KB, goal xor2
touch dir/filex5
mfssetgoal xor2 dir/filex5
dd if=/dev/zero of=dir/filex5 bs=64KiB count=1
	  length[dir/filex5]=65536
	    size[dir/filex5]=$((1 * header_size + 1 * block))
	realsize[dir/filex5]=$((3 * xor_header_size + 2 * block))

for field in length size realsize; do
	fieldsum=0
	for file in "${!length[@]}"; do
		eval "expected=\${$field[$file]}"
		actual=$(mfs_dir_info "$field" "$file")
		MESSAGE="$field for $file mismatch" expect_equals "$expected" "$actual"
		fieldsum=$((fieldsum + expected))
	done
	MESSAGE="$field for directory mismatch" expect_equals $fieldsum $(mfs_dir_info "$field" dir)
done
