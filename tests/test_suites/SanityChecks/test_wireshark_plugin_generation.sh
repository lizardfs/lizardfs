cd "$TEMP_DIR"
cp -a "$SOURCE_DIR/utils/wireshark/plugins/lizardfs" .
rm -f lizardfs/*.c

if is_program_installed python3 ; then
	assert_success lizardfs/generate.sh "$SOURCE_DIR/src/common/LFSCommunication.h"
	assert_success test -s lizardfs/packet-lizardfs.c
else
	echo "python3 is not installed on your system hence wireshark plugin won't be build."
fi
