timeout_set '1 minute'
CHUNKSERVERS=3 \
	MOUNTS=1 \
	USE_RAMDISK="YES" \
	LFSEXPORTS_EXTRA_OPTIONS="allcanchangequota,ignoregid" \
	setup_local_empty_lizardfs info

# Start Polonaise
lizardfs-polonaise-server --master-host=localhost \
	--master-port=${info[matocl]} \
	--bind-port=9090 &> /dev/null &
sleep 3
mnt="$TEMP_DIR/lfspolon"
mkdir -p "$mnt"

# fsname below is important. When the test is ended framework unmounts all the filesystems
# that match a given regex.
polonaise-fuse-client "$mnt" -o big_writes,allow_other,fsname=lfspolon &
MESSAGE="Client is not available" assert_eventually 'lfsdirinfo "$mnt"'

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
