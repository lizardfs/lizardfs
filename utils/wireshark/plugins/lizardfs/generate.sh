#!/bin/bash
set -o pipefail
set -eu

if (( $# != 1 )); then
    echo "Usage: $0 path/to/MFSCommunication.h"
    exit 1
fi

input_file=$(readlink -m "$1")
cd "$(dirname "$0")"

echo -e "#define PROTO_BASE 0\n#include \"$input_file\"" > includes.h

cat "$input_file" \
        | egrep -o '^#define (LIZ_)?(AN|CS|CL|MA|ML)TO(AN|CS|CL|MA|ML)_[A-Z0-9_]+' \
        | cut -d' ' -f2 \
        | grep -v CLTOMA_FUSE_DIR_REMOVED \
        | grep -v MATOCL_FUSE_NOTIFY \
        | sort -u \
        | sed -e 's/^/LIZARDFS_CONST_TO_NAME_ENTRY(/' -e 's/$/),/' \
        > dict_type-inl.h

cat "$input_file" \
        | egrep -o '^#define (ERROR_[A-Z0-9_]+|STATUS_OK)' \
        | cut -d' ' -f2 \
        | grep -v ERROR_STRINGS \
        | sort -u \
        | sed -e 's/^/LIZARDFS_CONST_TO_NAME_ENTRY(/' -e 's/$/),/' \
        > dict_status-inl.h

cat "$input_file" \
        | egrep -o '^#define TYPE_[A-Z]+' \
        | cut -d' ' -f2 \
        | sort -u \
        | sed -e 's/^/LIZARDFS_CONST_TO_NAME_ENTRY(/' -e 's/$/),/' \
        > dict_nodetype-inl.h

cat "$input_file" \
        | egrep -o '^#define GMODE_[A-Z]+' \
        | cut -d' ' -f2 \
        | grep -v GMODE_ISVALID \
        | sort -u \
        | sed -e 's/^/LIZARDFS_CONST_TO_NAME_ENTRY(/' -e 's/$/),/' \
        > dict_gmode-inl.h

cat "$input_file" \
        | egrep -o '^#define SMODE_[A-Z]+' \
        | cut -d' ' -f2 \
        | grep -v SMODE_ISVALID \
        | grep -v SMODE_.MASK \
        | sort -u \
        | sed -e 's/^/LIZARDFS_CONST_TO_NAME_ENTRY(/' -e 's/$/),/' \
        > dict_smode-inl.h

cat "$input_file" \
        | sed -r -e 's#(^|[^/])//([^/]|$).*#\1#' \
        | awk '/^#define (LIZ_)?..TO.._[A-Z0-9_]+/{print "Packet",$2} /^\/\/\//{sub(/\/\/\//,"DissectAs",$0); print}' \
        | sed -e 's/([^)]*)/BYTES/g' \
        | python3 make_dissector.py \
        > packet-lizardfs.c
