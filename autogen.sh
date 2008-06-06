#!/bin/sh

set -e

aclocal
autoconf
autoheader
automake -a -c
