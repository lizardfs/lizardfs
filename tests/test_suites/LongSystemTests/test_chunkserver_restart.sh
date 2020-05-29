#Timeout for initialization
timeout_set '200 seconds'

CHUNKSERVERS=64

CHUNKSERVERS=$CHUNKSERVERS \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

for ((i=1;i<=20;i++)) do
	echo 'Testing ith iteration =' $i

	for ((c=0;c<CHUNKSERVERS;c++)) do
		assert_eventually 'lizardfs_chunkserver_daemon $c isalive' '20 seconds'
	done

	for ((c=0;c<CHUNKSERVERS;c++)) do
		lizardfs_chunkserver_daemon $c stop &
	done
	wait

	for ((c=0;c<CHUNKSERVERS;c++)) do
		assert_eventually '! lizardfs_chunkserver_daemon $c isalive' '20 seconds'
	done

	for ((c=0;c<CHUNKSERVERS;c++)) do
		lizardfs_chunkserver_daemon $c start &
	done
	wait
done
