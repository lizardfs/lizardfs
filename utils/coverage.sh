#!/bin/bash
appname=$0
command=$1
builddir=$2
shift 2

usage() {
	{
		echo "Usage:"
		echo "$appname prepare <build-dir>"
		echo "    Prepares a CMake build directory to run tests with will generate"
		echo "    a code coverage report. This is needed, because LizardFS tests are"
		echo "    run with a different UID than the user who builds sources."
		echo "$appname generate-html <build-dir> <out-dir>"
		echo "    Generates a HTML code coverage report from the data collected during tests"
	} >&2
	exit 1
}

if [[ ! -f "$builddir/CMakeCache.txt" ]]; then
	usage
fi

if [[ $command == prepare ]]; then
	set -eu
	find "$builddir" -type d | xargs chmod a+w
	find "$builddir" -name '*.gcda' | xargs rm -f
elif [[ $command == generate-html ]]; then
	outdir=$1
	set -eu
	mkdir -p "$outdir"
	lcov --capture --directory . --output-file cov.raw
	lcov --remove cov.raw '/usr/*' '*/external/*' '*/tests/*' '*/utils/*' '*/devtools/*' -o cov.info
	genhtml cov.info --output-directory "$outdir" --no-branch-coverage
	rm -f cov.raw cov.info
else
	usage
fi
