timeout_set '3 minutes'
CHUNKSERVERS=3 \
	MOUNTS=1 \
	USE_RAMDISK="YES" \
	MFSEXPORTS_EXTRA_OPTIONS="allcanchangequota,ignoregid" \
	setup_local_empty_lizardfs info

# Start Polonaise
lizardfs-polonaise-server --master-host=localhost \
	--master-port=${info[matocl]} \
	--bind-port=9090 &> /dev/null &
sleep 3
mnt="$TEMP_DIR/mfspolon"
mkdir -p "$mnt"
polonaise-fuse-client "$mnt" -o allow_other &
MESSAGE="Client is not available" assert_eventually 'lizardfs dirinfo "$mnt"'

cd "$mnt"
for generator in $(metadata_get_all_generators | egrep -v "acl|xattr|trash"); do
	eval "$generator"
done
metadata_polonaise=$(DISABLE_PRINTING_XATTRS=yes metadata_print)
metadata_validate_files

cd "${info[mount0]}"
metadata_native=$(DISABLE_PRINTING_XATTRS=yes metadata_print)
metadata_validate_files

assert_no_diff "$metadata_polonaise" "$metadata_native"
