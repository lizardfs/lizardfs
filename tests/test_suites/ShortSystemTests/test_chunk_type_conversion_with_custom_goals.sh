timeout_set 2 minutes
WAIT_FOR_REPLICATION=60
NUMBER_OF_CHUNKSERVERS=11
CHUNKSERVER_LABELS="0,1,2:us|3,4,5:eu|6,7,8:cn"
MASTER_CUSTOM_GOALS="2 2: us us|3 3: us eu eu|4 4: us eu _ _|5 5: us eu us eu _|6 6: _ _ _ _ us us"
GOALS_TO_BE_TESTED="xor10 2 xor8 3 xor7 5 xor5 6 xor3 4 xor4"
VERIFY_FILE_CONTENT=YES

source test_suites/TestTemplates/test_chunk_type_conversion.inc
