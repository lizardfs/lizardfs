#!/usr/bin/env bash
set -eux

# Directories used by this script
output_dir=$(pwd)
source_dir=$(dirname "$0")
working_dir=/tmp/lizardfs_deb_working_directory

os_release="$(lsb_release -si)/$(lsb_release -sr)"

# Systemd is added by default, except for the following systems
case "$os_release" in
  Debian*/7*)  use_systemd=0 ;;
  Ubuntu*/12*) use_systemd=0 ;;
  Ubuntu*/14*) use_systemd=0 ;;
  *) use_systemd=1 ;;
esac

# Create an empty working directory and clone sources there to make
# sure there are no additional files included in the source package
rm -rf "$working_dir"
mkdir "$working_dir"
git clone "$source_dir" "$working_dir/lizardfs"

# Build packages.
cd "$working_dir/lizardfs"

# Move service files to debian/
cp -P rpm/service-files/* debian/

sed -i '1 s/-devel//g' debian/changelog
if [[ $use_systemd == 0 ]]; then
	dpkg-buildpackage -uc -us -F -R'debian/rules-nosystemd'
else
	dpkg-buildpackage -uc -us -F
fi

# Copy all the created files and clean up
cp "$working_dir"/lizardfs?* "$output_dir"
rm -rf "$working_dir"
