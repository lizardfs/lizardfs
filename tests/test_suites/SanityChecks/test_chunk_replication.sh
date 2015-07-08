test_timeout="30 seconds"
replication_timeout="15 seconds"
number_of_chunkservers=5
goals="3 xor3"
verify_file_content=NO

source $(readlink -m test_suites/ShortSystemTests/test_chunk_replication.sh)
