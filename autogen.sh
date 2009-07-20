#!/bin/sh

set -ex

# MacOS
if [ -d /opt/local/share/aclocal ]; then
	MACROSINC=-I/opt/local/share/aclocal
else
	MACROSINC=
fi

if [ -x /usr/bin/glibtoolize ]; then
	glibtoolize --copy --force
else
	libtoolize --copy --force
fi
aclocal $MACROSINC
autoconf
autoheader
automake -a -c --foreign
