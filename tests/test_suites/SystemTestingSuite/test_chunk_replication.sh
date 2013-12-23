timeout_set 120 seconds
WAIT_FOR_REPLICATION=90

NUMBER_OF_CHUNKSERVERS=7
GOALS_TO_BE_TESTED="2 4 xor2 xor3"
VERIFY_FILE_CONTENT=YES

source test_suites/TestTemplates/test_chunk_replication.inc
