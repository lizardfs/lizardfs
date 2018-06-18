# Makes a proxy which accepts only 'counter' incoming bytes
# and if the limit is reached, increases it twice
start_proxy() {
	local listen_port=$1
	local server_port=$2
	local initial_counter=$3
	local counter_file="$TEMP_DIR/proxycounter_${listen_port}"
	local ip=$(get_ip_addr)
	local script="
		read counter < '$counter_file' || counter=$initial_counter # in case of a read/write race
		tempf=\$(tempfile -d '$RAMDISK_DIR')
		stdbuf -o0 head -c \$counter | stdbuf -o0 tee \$tempf | socat stdio tcp\:$ip\:$server_port
		if test \$(cat \$tempf | wc --bytes) -eq \$counter ; then
			# We have reached the limit -- let's increase it
			echo \$(( 2 * counter )) > '$counter_file'
		fi
		rm -f \$tempf
	"

	echo $initial_counter > "$counter_file"
	( socat tcp-listen:${listen_port},reuseaddr,fork "system:${script}" & )
}

timeout_set 10 minutes
assert_program_installed socat
CHUNKSERVERS=4 \
	MOUNTS=10 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

# Start failproxies on each chunkserver with different 'initial counter' parameters
for csid in {0..3}; do
	port=${info[chunkserver${csid}_port]}
	lizardfs_chunkserver_daemon $csid stop
	start_proxy $port $((port + 1000)) $((1883 * (4 + csid)))
	sleep 2
	LD_PRELOAD="$LIZARDFS_ROOT/lib/libredirect_bind.so" lizardfs_chunkserver_daemon $csid start
done
lizardfs_wait_for_all_ready_chunkservers

# Create a xor3 directory for tests
mkdir "${info[mount0]}/dir"
lizardfs setgoal xor3 "${info[mount0]}/dir"

# Create a small file by writing it using 10 clients concurrently
src="$RAMDISK_DIR/src"
FILE_SIZE=2000K file-generate "$src"
touch "${info[mount2]}/dir/small"
for i in {0..9}; do
	dd if="$src" of="${info[mount${i}]}/dir/small" \
			bs=200K count=1 seek=$i skip=$i conv=notrunc 2>/dev/null &
done
wait

# Now create a big file using a single mountpoint
FILE_SIZE=200M file-generate "${info[mount1]}/dir/big"

# Validate all parts (including parity parts)
MESSAGE="Validating data" expect_success file-validate "${info[mount2]}"/dir/*
for csid in 0 1; do
	lizardfs_chunkserver_daemon $csid stop
	MESSAGE="Validating data (CS$csid is down)" expect_success file-validate "${info[mount3]}"/dir/*
	LD_PRELOAD="$LIZARDFS_ROOT/lib/libredirect_bind.so" lizardfs_chunkserver_daemon $csid start
	lizardfs_wait_for_all_ready_chunkservers
done
