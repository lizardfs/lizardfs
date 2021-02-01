SLEEP_TIME=3 # in seconds, after some commands

function start_services_12() {
	assert_eventually mfsmaster-v12
	assert_eventually mfschunkserver-v12
	for (( i = 2; i <= $CS_N; ++i )); do
		assert_eventually mfschunkserver-v12-$i
	done
	assert_eventually mfsmount-v12
	echo -e "\n\nStarted 3.12 Services.\n"
}

function stop_services_12() {
	cd ~
	fusermount -u $MNT_DIR
	sleep $SLEEP_TIME
	mfschunkserver-v12 stop
	sleep $SLEEP_TIME
	for (( i = 2; i <= $CS_N; ++i )); do
		mfschunkserver-v12-$i stop
		sleep $SLEEP_TIME
	done
	mfsmaster-v12 stop
	sleep $SLEEP_TIME
	echo -e "\n\nStopped 3.12 Services.\n"
}

function start_services_13() {
	assert_eventually mfsmaster
	assert_eventually mfschunkserver
	for (( i = 2; i <= $CS_N; ++i )); do
		assert_eventually mfschunkserver-$i
	done
	assert_eventually mfsmount
	echo -e "\n\nStarted 3.13 Services.\n"
}

function stop_services_13() {
	cd ~
	fusermount -u $MNT_DIR
	sleep $SLEEP_TIME
	mfschunkserver stop
	sleep $SLEEP_TIME
	for (( i = 2; i <= $CS_N; ++i )); do
		mfschunkserver-$i stop
		sleep $SLEEP_TIME
	done
	mfsmaster stop
	sleep 3
	echo -e "\n\nStopped 3.13 Services.\n"
}

function clear_chunkserver_data() {
	cd $DISK_DIR
	rm -rf vd0 vd1 vd2 vd3 vd4
	mkdir vd0 vd1 vd2 vd3 vd4
	echo -e "\n\nCleared /dev Chunkservers data.\n"
}

function clear_master_metadata() {
	local datadir=/home/trzysiek/usr/local/var/lib
	rm -rf $datadir/lizardfs
	mkdir $datadir/lizardfs
	for i in {1..4}; do
		rm -rf $datadir/lizardfs-$i
		mkdir $datadir/lizardfs-$i
	done
	cp $datadir/metadata.mfs $datadir/lizardfs
}

# Helper unimportant functions

function wait_for() {
	local goal=$1
	local time_limit="$2"
	local end_ts=$(date +%s%N -d "$time_limit")
	local i=0
	while (( $(date +%s%N) < end_ts )); do
		if eval "$goal"; then
			return 0
		fi
		sleep 1
		((i++))
		if [[ $((i % 10)) == "0" ]]; then
			echo $i
		fi
	done
	if eval "$goal"; then
		return 0
	fi
	return 1
}

function assert_eventually() {
	local command=$1
	local timeout="300 seconds"
	if ! wait_for "$command" "$timeout"; then
		 echo "ZLE"
		 FINISHED="true"
	fi
}
