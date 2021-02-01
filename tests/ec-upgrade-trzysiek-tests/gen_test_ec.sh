DISK_DIR=/home/trzysiek/dev
MNT_DIR=/home/trzysiek/mnt
N_10M=100
N_100M=20
N_1G=2
N_5G=1
CS_N=3
EC=ec21
EC_DIR=$EC

function gen_all_files() {
	rm -rf $MNT_DIR/$EC_DIR
	mkdir -p $MNT_DIR/$EC_DIR
	lizardfs-v12 setgoal $EC $MNT_DIR/$EC_DIR
	gen_some_files 10M 10 $N_10M
	gen_some_files 100M 100 $N_100M
	gen_some_files 1G 1000 $N_1G
	gen_some_files 5G 5000 $N_5G
	echo -e "\n\nGenerated all files.\n"
}

# name filesize(in MB) n_files
function gen_some_files() {
	name=$1
	f_size_mb=$2
	n_files=$3

	cd $MNT_DIR/$EC_DIR
	mkdir $name
	cd $name
	for i in $(seq $n_files); do
		dd if=/dev/zero of=$i bs=1M count=$f_size_mb
	done
}

function check_if_chunks_missing_and_update_FINISHED() {
	# Check data on disks
	local N=$(( CS_N - 1 ))
	for i in {0..$N}; do
		local OK_N=$((N_10M + N_100M*2 + N_1G*16 + N_5G*79 + 1))
		local AC_N=$(find $DISK_DIR/vd$i -type f | wc -l)
		echo "vd$i has $AC_N files, should be $OK_N"
		if [[ $OK_N != $AC_N ]]; then
			FINISHED="true"
			exit 0
		fi
	done

	# Check chunk info
	for i in $(find $MNT_DIR/$EC_DIR/10M -type f); do
		local OK_N="3"
		local AC_N=$(lizardfs fileinfo $i | grep "copy" | wc -l)
		echo -ne $AC_N ""
		if [[ $AC_N != $OK_N ]]; then
			FINISHED="true"
			exit 0
		fi
	done
	echo ""
	for i in $(find $MNT_DIR/$EC_DIR/100M -type f); do
		local OK_N="6"
		local AC_N=$(lizardfs fileinfo $i | grep "copy" | wc -l)
		echo -ne $AC_N ""
		if [[ $AC_N != $OK_N ]]; then
			FINISHED="true"
			exit 0
		fi
	done
	echo ""
	for i in $(find $MNT_DIR/$EC_DIR/1G -type f); do
		local OK_N="48"
		local AC_N=$(lizardfs fileinfo $i | grep "copy" | wc -l)
		echo -ne $AC_N ""
		if [[ $AC_N != $OK_N ]]; then
			FINISHED="true"
			exit 0
		fi
	done
	echo ""
	for i in $(find $MNT_DIR/$EC_DIR/5G -type f); do
		local OK_N="237"
		local AC_N=$(lizardfs fileinfo $i | grep "copy" | wc -l)
		echo -ne $AC_N ""
		if [[ $AC_N != $OK_N ]]; then
			FINISHED="true"
			exit 0
		fi
	done
	echo ""
}

################### SCRIPT HERE ####################
####################################################

. utils.sh

FINISHED="false"
while [[ $FINISHED != "true" ]]; do
	# stopping ok even if they don't run
	stop_services_13
	clear_master_metadata
	clear_chunkserver_data
	sleep 5m
	start_services_12
	gen_all_files
	stop_services_12
	start_services_13
	echo -e "\n\nSleeping for 25min - take care!\n"
	sleep 25m
	check_if_chunks_missing_and_update_FINISHED
	echo -e "\n\nKoniec jednej iteracji!\n"
	FINISHED="true"
done
