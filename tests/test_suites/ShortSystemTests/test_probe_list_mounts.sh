MOUNTS=4 \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

mounts=$(lizardfs-probe list-mounts --porcelain --verbose localhost "${info[matocl]}")
expect_equals "4" $(wc -l <<< "$mounts")
for i in {1..4}; do
	expect_equals \
		"$i ${info[mount$((i - 1))]} $LIZARDFS_VERSION / 0 0 999 999 no yes no no no 1 40 - -" \
		"$(sed -n "${i}p" <<< "$mounts" | cut -d' ' -f 1,3-)"
done
