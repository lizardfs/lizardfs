#!/bin/bash
set -eux

# Directories used by this script
output_dir=$(pwd)
source_dir=$(dirname "$0")
working_dir=/tmp/lizardfs_deb_working_directory

# Create an empty working directory and clone sources there to make
# sure there are no additional files included in the source package
rm -rf "$working_dir"
mkdir "$working_dir"
git clone "$source_dir" "$working_dir/lizardfs"

# Build packages
cd "$working_dir/lizardfs"
dpkg-buildpackage -uc -us -F

# Copy all the created files and clean up
cp "$working_dir"/lizardfs?* "$output_dir"
rm -rf "$working_dir"
