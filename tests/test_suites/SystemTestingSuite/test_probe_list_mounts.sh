CHUNKSERVERS=4 \
	MOUNTS=4 \
	MASTER_EXTRA_CONFIG="REPLICATIONS_DELAY_INIT = 100000" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

list_mounts() {
	lizardfs-probe list-mounts localhost "${info[matocl]}" --porcelain --verbose
}

mounts=$(list_mounts)
expect_equals "4" $(wc -l <<< "$mounts")
mount_points=""
for i in {1..4}; do
	expect_equals \
		"$i ${info[mount$((i - 1))]} $LIZARDFS_VERSION / 0 0 999 999 no yes no no no 1 9 - -" \
		"$(sed -n "${i}p" <<< "$mounts" | cut -d' ' -f 1,3-)"
done
