timeout_set '40 seconds'
assert_program_installed socat

# start_proxy <listen> <forward>
# Starts a proxy which listens on <listen> port, forwards all the traffic to <forward> port
# and pauses the traffic when file $TEMP_DIR/gogo does not exist
start_proxy() {
	local ip=$(get_ip_addr)
	local limiter_script="socat stdio tcp\:$ip\:$2\,rcvbuf=1024\,sndbuf=1024 | `
			`while true; do `
				`if test -e \"$TEMP_DIR/gogo\"; then `
					`dd bs=1024 count=1 2>/dev/null; else sleep 0.1; `
				`fi; `
			`done"
	( socat "tcp-listen:$1,reuseaddr,rcvbuf=1024,sndbuf=1024" "system:${limiter_script}" & )
}

CHUNKSERVERS=1 \
	MASTERSERVERS=2 \
	USE_RAMDISK="YES" \
	MASTER_EXTRA_CONFIG="MASTER_TIMEOUT = 10000" \
	setup_local_empty_lizardfs info

m=${info[master0_data_path]} # master's working directory
s=${info[master1_data_path]} # shadow's working directory

# Create a proxy which will pause the traffic from master to shadow and modify shadow's
# config to use this proxy.
get_next_port_number proxy_port
touch "$TEMP_DIR/gogo";
start_proxy "$proxy_port" "${info[matoml]}"
sed -i -e "s/^MASTER_PORT.*/MASTER_PORT = $proxy_port/" "${info[master1_cfg]}"

# Start shadow master and wait until it synchronizes
assert_success lizardfs_master_n 1 start
assert_eventually "lizardfs_shadow_synchronized 1"

# Pause traffic from master to shadow and generate enough changes to make write() to socket block
rm "$TEMP_DIR/gogo"
mkdir "${info[mount0]}"/directory_with_name_long_enough_to_generate_a_big_changelog_entry{1..4000}

# Send SIGTERM to the master server and 10 seconds later resume the traffic
begin_ts=$(timestamp)
( sleep 10; touch "$TEMP_DIR/gogo"; ) &
assert_success lizardfs_master_daemon stop
end_ts=$(timestamp)

# The traffic was paused for 10 seconds, let's check if we didn't succeed too soon
duration=$((end_ts - begin_ts))
assert_less_or_equal 10 $duration

# Let all the buffers be flushed and verify if all the changelogs were transferred
assert_eventually_equals 'get_changes "$m" | tail' 'get_changes "$s" | tail'
