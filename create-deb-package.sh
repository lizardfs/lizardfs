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

version="${VERSION_LONG_STRING:-"0.0.0-$(date -u +"%Y%m%d-%H%M%S")-devel"}"
export version

# Generate entry at the top of the changelog, needed to build the package
last_header=$(grep lizardfs debian/changelog  | grep urgency | head -n 1)
status=$(echo "${version}" | cut -d'-' -f4)
package_name=$(echo "${last_header}" | awk '{print $1}')
changelog_version="${version%%-*}"
urgency=$(echo "${last_header}" | sed -e 's/^.*urgency=\(\w*\).*$/\1/')
(cat <<EOT
${package_name} (${changelog_version}) ${status}; urgency=${urgency}

  * Vendor ${status} release.
  * commit: $(git rev-parse HEAD)

 -- dev.lizardfs.org package-builder <dev@lizardfs.org>  $(date -R)

EOT
) | cat - debian/changelog > debian/changelog.tmp && mv debian/changelog.tmp debian/changelog

# Build packages.
dpkg_genchanges_params="-uc -us -F --changes-option=-Dversion=${version}"
if [[ $use_systemd == 0 ]]; then
	# shellcheck disable=SC2086
	dpkg-buildpackage ${dpkg_genchanges_params} -R'debian/rules-nosystemd'
else
	# shellcheck disable=SC2086
	dpkg-buildpackage ${dpkg_genchanges_params}
fi

# Copy all the created files and clean up

cp "$working_dir"/lizardfs?* "$output_dir"
rm -rf "$working_dir"
