#!/bin/sh

set -e

# MacOS
if [ -d /opt/local/share/aclocal ]; then
	MACROSINC=-I/opt/local/share/aclocal
else
	MACROSINC=
fi

aclocal $MACROSINC
autoconf
autoheader
automake -a -c
