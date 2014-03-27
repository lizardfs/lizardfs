cd "$TEMP_DIR"
cp -a "$SOURCE_DIR/utils/wireshark/plugins/lizardfs" .
rm -f lizardfs/*.c

assert_success lizardfs/generate.sh "$SOURCE_DIR/src/common/MFSCommunication.h"
assert_success test -s lizardfs/packet-lizardfs.c
