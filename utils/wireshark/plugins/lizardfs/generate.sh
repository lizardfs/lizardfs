#!/bin/bash
set -eu

if (( $# != 1 )); then
	echo "Usage: $0 path/to/LFSCommunication.h"
	exit 1
fi

input_file=$(readlink -m "$1")
cd "$(dirname "$0")"

# Generate the includes.h file which properly includes all the definitions of LizardFS constants
{
	echo "#define PROTO_BASE 0"
	echo "#define LIZARDFS_WIRESHARK_PLUGIN 1"
	echo "#include \"$input_file\"" # LFSCommunication.h
} > includes.h

# Generate the packet-lizardfs.c file
python3 make_dissector.py < "$input_file" > packet-lizardfs.c
