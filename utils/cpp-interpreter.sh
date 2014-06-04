#!/bin/bash
set -e
# Source file is the first argument. Read it but cut off the #!/... line
source="$(cat "$1" | grep -v '^#!')"
shift
[[ -n "$source" ]] || exit 1

# Compute hash of the source and use it in the name of the executable, so that the name will be
# the same when running the same script many times. This will make it possible to cache the binary
# cached in TEMP_DIR (in tests) or in /tmp (when running by hand)
hash=$(md5sum <<< "$source" | awk '{print $1}')
executable="${TEMP_DIR:-/tmp}/cached_c_$hash"
if ! [[ -x "$executable" ]]; then
	c++ -xc++ -o "$executable" - <<< "$source"
fi
"$executable" "$@"
