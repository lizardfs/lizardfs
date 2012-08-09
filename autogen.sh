#!/bin/sh

set -ex

# MacOS
if [ -d /opt/local/share/aclocal ]; then
	MACROSINC=-I/opt/local/share/aclocal
else
	MACROSINC=
fi

if [ -x /opt/local/bin/glibtoolize ]; then
	/opt/local/bin/glibtoolize --copy --force
elif [ -x /usr/bin/glibtoolize ]; then
	/usr/bin/glibtoolize --copy --force
else
	libtoolize --copy --force
fi
aclocal -I m4 $MACROSINC
autoconf
autoheader
automake -a -c --foreign
