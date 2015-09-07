timeout_set 2 minutes
WAIT_FOR_REPLICATION=60
NUMBER_OF_CHUNKSERVERS=10
GOALS_TO_BE_TESTED="xor9 2 xor8 3 xor7 5 xor5 6 xor3 4 9 xor4 xor6 xor2"
VERIFY_FILE_CONTENT=YES

source test_suites/TestTemplates/test_chunk_type_conversion.inc
