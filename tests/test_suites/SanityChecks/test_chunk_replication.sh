timeout_set 20 seconds
WAIT_FOR_REPLICATION=15

NUMBER_OF_CHUNKSERVERS=3
GOALS_TO_BE_TESTED="1 2 3"
VERIFY_FILE_CONTENT=NO

source test_suites/TestTemplates/test_chunk_replication.inc
