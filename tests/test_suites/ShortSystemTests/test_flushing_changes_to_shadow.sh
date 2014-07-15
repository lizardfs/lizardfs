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

# Pause traffic from master to shadow
rm "$TEMP_DIR/gogo"

mkdir "${info[mount0]}"/directory_with_name_long_enough_to_generate_a_big_changelog_entry{1..10000}
begin_ts=$(timestamp)
# Wait 3 seconds and resume the traffic between master and shadow
( sleep 3; touch "$TEMP_DIR/gogo"; ) &
assert_success lizardfs_master_daemon stop

assert_eventually_equals 'get_changes "$m" |tail' 'get_changes "$s" |tail'
end_ts=$(timestamp)
duration=$((end_ts - begin_ts))
# The traffic was paused for 3 seconds, let's check if we didn't succeed too soon
assert_less_or_equal 3 $duration
