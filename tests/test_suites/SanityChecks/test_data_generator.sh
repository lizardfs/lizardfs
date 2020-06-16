timeout_set 2 minutes

cd "$TEMP_DIR"

FILE_SIZE=64 SEED=666 file-generate file_seed666
FILE_SIZE=64 SEED=42  file-generate file_seed42

assert_failure diff file_seed666 file_seed42

cp file_seed666 copied_file
SEED=997 file-overwrite copied_file
assert_failure diff file_seed666 copied_file

SEED=666 assert_success file-validate file_seed666
SEED=997 assert_success file-validate copied_file
SEED=42  assert_success file-validate file_seed42
SEED=997 assert_failure file-validate file_seed666

big_size=100M
SEED=2137 FILE_SIZE=$big_size BLOCK_SIZE=2135 file-generate big_file &
# wait until the file exists (can be opened)
until [ -f big_file ]; do sleep 1; done
SEED=2137 assert_success file-validate-growing big_file $big_size
