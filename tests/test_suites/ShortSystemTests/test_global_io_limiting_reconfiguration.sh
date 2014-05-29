timeout_set 1 minute

# Create a config file with a limit of 1 MB/s for all processes
iolimits="$TEMP_DIR/iolimits.cfg"
echo "limit unclassified 1024" > "$iolimits"

E=11
if valgrind_enabled; then
	E=$((5 * E))
fi

CHUNKSERVERS=3 \
	USE_RAMDISK=YES \
	MASTER_EXTRA_CONFIG="GLOBALIOLIMITS_FILENAME = $iolimits" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info

truncate -s1M "${info[mount0]}/file"

# consume accumulated limit
cat "${info[mount0]}/file" >/dev/null

{
	sleep 2
	echo "limit unclassified 2048" > "$iolimits"
	lizardfs_master_daemon reload
} &

start=$(nanostamp)
for i in {1..6}; do
	cat "${info[mount0]}/file" >/dev/null
done
end=$(nanostamp)

expected=$((4 * 1000 * 1000 * 1000))
time=$((end - start))
abserr=$((time - expected))
relerr=$((100 * abserr / expected))
echo $expected $time $abserr $relerr
assert_near 0 $relerr $E
