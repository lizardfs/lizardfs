timeout_set 1 minute

# Create a config file with a limit of 10 MB/s for all processes
iolimits="$TEMP_DIR/iolimits.cfg"
echo "limit unclassified 10240" > "$iolimits"

# number of mounts
N=5
# number of rounds
R=3
# tolerated relative error (in %)
E=11
if valgrind_enabled; then
	E=$((5 * E))
fi

CHUNKSERVERS=3 \
	MOUNTS=$N \
	USE_RAMDISK=YES \
	MASTER_EXTRA_CONFIG="GLOBALIOLIMITS_FILENAME = $iolimits" \
	MOUNT_EXTRA_CONFIG="lfscachemode=NEVER" \
	setup_local_empty_lizardfs info

truncate -s10M "${info[mount0]}/file"

# consume accumulated limit
cat "${info[mount0]}/file" >/dev/null

for ((round=0; round < R; round++)); do
	for ((mount=0; mount < N; mount++)); do
		start=$(nanostamp)
		cat "${info[mount$mount]}/file" >/dev/null
		end=$(nanostamp)
		expected=$((1000 * 1000 * 1000))
		time=$((end - start))
		abserr=$((time - expected))
		relerr=$((100 * abserr / expected))
		echo $expected $time $abserr $relerr
		assert_near 0 $relerr $E
	done
done
