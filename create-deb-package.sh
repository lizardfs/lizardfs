#!/usr/bin/env bash
set -eux

export LIZARDFS_OFFICIAL_BUILD=NO

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

cd "$working_dir/lizardfs"

# Move service files to debian/

cp -P rpm/service-files/* debian/

sed -i '1 s/-devel//g' debian/changelog

last_header=$(cat debian/changelog | grep lizardfs | grep urgency | head -n 1)
v_tmp=${last_header%)*}
version=${v_tmp#* (} # extracted version number
if [[ -v BUILD_DATE ]]; then
	version=${version}-${BUILD_DATE}
fi
export version

# Generate entry at the top of the changelog, needed to build the package
header=$(echo $last_header | sed -e "s/stable/$(lsb_release -sc)/" -e "s/\((.*\))/\1.dev)/")
changes=" * Vendor test release.\n  * commit: $(git rev-parse HEAD)"
signature="-- dev.lizardfs.org package-builder <dev@lizardfs.org>  $(date -R)"
echo -e "$header" "\n\n" "$changes" "\n\n" "$signature" "\n" \
	| cat - debian/changelog > debian/changelog.tmp && mv debian/changelog.tmp debian/changelog

# Build packages.
dpkg_genchanges_params="-uc -us -F --changes-option=-Dversion=${version}"
if [[ $use_systemd == 0 ]]; then
	dpkg-buildpackage ${dpkg_genchanges_params} -R'debian/rules-nosystemd'
else
	dpkg-buildpackage ${dpkg_genchanges_params}
fi

# Copy all the created files and clean up

cp "$working_dir"/lizardfs?* "$output_dir"
rm -rf "$working_dir"
