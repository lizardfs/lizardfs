timeout_set 2 minutes

MOUNTS=2 \
	CHUNKSERVERS=1 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER,mfsacl" \
	MOUNT_1_EXTRA_EXPORTS="allcanchangequota,mapall=lizardfstest_6:lizardfstest_4" \
	setup_local_empty_lizardfs info

normal="${info[mount0]}"
mapall="${info[mount1]}"

stat_ug() {
	stat --format='%U:%G' $*
}

# quickly check if IDs are remapped properly in both ways
touch "$normal/normal"
touch "$mapall/mapall"
expect_equals 'lizardfstest:lizardfstest' $(stat_ug "$normal/normal")
expect_equals 'lizardfstest_6:lizardfstest_4' $(stat_ug "$normal/mapall")
expect_equals 'root:root' $(stat_ug "$mapall/normal")
expect_equals 'lizardfstest:lizardfstest' $(stat_ug "$mapall/mapall")
rm "$normal/normal"
rm "$mapall/mapall"

# run metadata generators on 'mapall' mount (skip uids_gids, it doesn't work)
cd "$mapall"
for generator in $(metadata_get_all_generators |grep -v metadata_generate_uids_gids); do
	eval "$generator"
done

# check UIDs and GIDs
cd "$normal"
find -mindepth 1 | while read i; do
	if lizardfs geteattr "$i" |grep -q noowner; then
		expect_equals 'lizardfstest:lizardfstest' $(stat_ug "$i")
	else
		expect_equals 'lizardfstest_6:lizardfstest_4' $(stat_ug "$i")
	fi
done
