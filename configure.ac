#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.60)
dnl AC_PREREQ(2.60)
AC_INIT([MFS], [1.6.27], [bugs@moosefs.com])
dnl AC_INIT([MFS], [1.7.0], [bugs@moosefs.com])
dnl AC_CONFIG_SRCDIR([MFSCommunication.h])
AC_CONFIG_HEADER([config.h])
AC_CANONICAL_TARGET
AM_INIT_AUTOMAKE([1.10 foreign])

versmaj=$(echo $PACKAGE_VERSION | cut -d. -f1)
versmid=$(echo $PACKAGE_VERSION | cut -d. -f2)
versmin=$(echo $PACKAGE_VERSION | cut -d. -f3)
AC_DEFINE_UNQUOTED([VERSMAJ], [$versmaj], [Major MFS version])
AC_DEFINE_UNQUOTED([VERSMID], [$versmid], [Middle MFS version])
AC_DEFINE_UNQUOTED([VERSMIN], [$versmin], [Minor MFS version])
AC_DEFINE_UNQUOTED([VERSHEX], [(($versmaj)*0x10000+($versmid)*0x100+($versmin))], [Full MFS version as a hex number])

# Checks for programs.
AC_PROG_CC
AC_PROG_CXX
AC_PROG_LN_S
AC_PROG_LIBTOOL

if test "$GCC" = "yes"; then
	CFLAGS="-std=c99 $CFLAGS -Wall -Wextra -Wshadow -pedantic -fwrapv -O2"
dnl	CFLAGS="-O0 -W -Wall -Wshadow"
fi

case $target_os in
  *linux*)
	fuse=maybe
	CPPFLAGS="$CPPFLAGS -D_GNU_SOURCE -DDEFAULT_SUGID_CLEAR_MODE_EXT"
	;;
  *solaris*)
	# OpenSolaris has fuse available and is still identified as "solaris"
	fuse=maybe
	CPPFLAGS="$CPPFLAGS -D__EXTENSIONS__ -D_POSIX_PTHREAD_SEMANTICS -DDEFAULT_SUGID_CLEAR_MODE_OSX"
	;;
  *darwin*)
	fuse=maybe
	# MacFUSE installs to different prefix than pkgconfig
	if test -f /usr/local/lib/pkgconfig/fuse.pc ; then
		PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
		export PKG_CONFIG_PATH
	fi
	CPPFLAGS="$CPPFLAGS -DDEFAULT_SUGID_CLEAR_MODE_OSX"
	;;
  *bsd*)
	fuse=maybe
	CPPFLAGS="$CPPFLAGS -DDEFAULT_SUGID_CLEAR_MODE_BSD"
	;;
  *)
	fuse=no
	;;
esac

AX_PTHREAD(, [AC_MSG_ERROR([Posix threads not found])])
dnl AC_CHECK_LIB([pthread], [pthread_create])

# Checks for libraries.
AC_SEARCH_LIBS([socket], [socket])
AC_SEARCH_LIBS([getaddrinfo], [nsl])
AC_SEARCH_LIBS([round], [m])

# Checks for header files.
AC_HEADER_DIRENT
AC_HEADER_STDC
AC_HEADER_SYS_WAIT
AC_CHECK_HEADERS([arpa/inet.h fcntl.h inttypes.h limits.h netdb.h netinet/in.h stddef.h stdlib.h string.h sys/socket.h sys/statvfs.h sys/time.h syslog.h unistd.h],, [AC_MSG_ERROR([One of required headers was not found])])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_C_BIGENDIAN
AC_C_INLINE
AC_C_VOLATILE
AC_HEADER_STDBOOL
AC_TYPE_INT8_T
AC_TYPE_UINT8_T
AC_TYPE_INT16_T
AC_TYPE_UINT16_T
AC_TYPE_INT32_T
AC_TYPE_UINT32_T
AC_TYPE_INT64_T
AC_TYPE_UINT64_T
AC_TYPE_UID_T
AC_TYPE_MODE_T
AC_TYPE_OFF_T
AC_TYPE_PID_T
AC_TYPE_SIZE_T
AC_STRUCT_ST_BLOCKS
AC_CHECK_MEMBERS([struct stat.st_rdev])
AC_CHECK_MEMBERS([struct stat.st_birthtime])
AC_CHECK_MEMBERS([struct stat.st_blksize])
AC_CHECK_MEMBERS([struct stat.st_flags])

AC_HEADER_TIME
AC_STRUCT_TM
AC_CHECK_MEMBERS([struct tm.tm_gmtoff],,, [
#ifdef TM_IN_SYS_TIME
#  include <sys/time.h>
#else
#  include <time.h>
#endif
])

AC_SYS_LARGEFILE

# Checks for library functions.
AC_FUNC_STRERROR_R
AC_FUNC_CLOSEDIR_VOID
AC_FUNC_FORK
AC_FUNC_LSTAT
AC_FUNC_MALLOC
AC_FUNC_MEMCMP
AC_FUNC_REALLOC
dnl AC_FUNC_SELECT_ARGTYPES
AC_FUNC_STAT
# required functions
AC_CHECK_FUNCS([atexit bzero ftruncate getaddrinfo getpass gettimeofday memmove memset mkdir realpath poll socket strchr strdup strtol strtoul ftello fseeko],, [AC_MSG_ERROR([One of required functions was not found])])

# optional error conversion functions
AC_CHECK_FUNCS([strerror perror])

# optional system functions
AC_CHECK_FUNCS([dup2 mlockall getcwd])
AC_CHECK_HEADERS([sys/mman.h])

# optional I/O functions
AC_CHECK_FUNCS([pread pwrite readv writev])

# optional resource usage function and headers
AC_CHECK_FUNCS([getrusage setitimer])
AC_CHECK_HEADERS([sys/rusage.h sys/resource.h])
AC_CHECK_MEMBERS([struct rusage.ru_maxrss],,, [
#include <sys/time.h>
#ifdef HAVE_GETRUSAGE
#  ifdef HAVE_SYS_RESOURCE_H
#    include <sys/resource.h>
#  endif
#  ifdef HAVE_SYS_RUSAGE_H
#    include <sys/rusage.h>
#  endif
#endif
])

dnl optional thread functions
dnl AC_CHECK_FUNCS([pthread_spin_lock])

dnl optional interfaces (Linux, FreeBSD)
dnl AC_CHECK_FUNCS([fopencookie funopen])

dnl this is work in progress
dnl AC_ARG_ENABLE([lightmfs], [AS_HELP_STRING([--enable-lightmfs], [Build special version])])

AC_ARG_ENABLE([mfsmaster], [AS_HELP_STRING([--disable-mfsmaster], [Don't build mfsmaster])])
AC_ARG_ENABLE([mfschunkserver], [AS_HELP_STRING([--disable-mfschunkserver], [Don't build mfschunkserver])])
AC_ARG_ENABLE([mfsmount], [AS_HELP_STRING([--disable-mfsmount], [Don't build mfsmount])])
AC_ARG_ENABLE([mfscgi], [AS_HELP_STRING([--disable-mfscgi], [Don't install CGI scripts])])
AC_ARG_ENABLE([mfscgiserv], [AS_HELP_STRING([--disable-mfscgiserv], [Don't install CGI server])])
AC_ARG_WITH([mfscgi-dir], [AS_HELP_STRING([--with-mfscgi-dir=CGIDIR], [Choose CGI directory (default=DATADIR/mfscgi)])],
	[CGIDIR=$withval], [CGIDIR=$datadir/mfscgi])
AC_ARG_WITH([mfscgiserv-dir], [AS_HELP_STRING([--with-mfscgiserv-dir=CGISERVDIR], [Choose CGI directory (default=SBINDIR)])],
	[CGISERVDIR=$withval], [CGISERVDIR=$sbindir])
AC_ARG_WITH([zlib], [AS_HELP_STRING([--without-zlib], [Don't use zlib for PNG compression])], [use_zlib=$withval], [use_zlib=yes])

dnl if test "$enable_mfsmaster" = "no" -a "$enable_mfschunkserver" = "no" -a "$enable_mfsmount" = "no" -a "$enable_mfscgi" = "no" -a "$enable_mfscgiserv" = "no"; then
dnl	echo "**********************************"
dnl	echo "* no modules choosen - build all *"
dnl	echo "**********************************"
dnl	enable_mfsmaster=yes
dnl	enable_mfschunkserver=yes
dnl	enable_mfsmount=yes
dnl	enable_mfscgi=yes
dnl	enable_mfscgiserv=yes
dnl fi

if test "$enable_mfscgi" != "no"; then
	AM_PATH_PYTHON([2.0],,[:])
	if test "$PYTHON" = ":"; then
		enable_mfscgi=no
	fi
fi
if test "$enable_mfscgi" = "no"; then
	enable_mfscgiserv=no
fi
AM_CONDITIONAL([INSTALL_CGI], [test "$enable_mfscgi" != "no"])
AM_CONDITIONAL([INSTALL_CGISERV], [test "$enable_mfscgiserv" != "no"])
AC_SUBST([CGIDIR])
AC_SUBST([CGISERVDIR])

AM_CONDITIONAL([BUILD_MASTER], [test "$enable_mfsmaster" != "no"])
AM_CONDITIONAL([BUILD_CHUNKSERVER], [test "$enable_mfschunkserver" != "no"])
if test "$fuse" != "no" -a "enable_mfsmount" != "no"; then
	PKG_PROG_PKG_CONFIG
	if test -n "$PKG_CONFIG"; then
		PKG_CHECK_MODULES([FUSE], [fuse >= 2.6], , [fuse=no])
	else
		echo "no pkg-config - can't check FUSE version"
		fuse=no
	fi
fi
if test "$fuse" = "no"; then
	if test "$enable_mfsmount" = "yes"; then
		if test -n "$PKG_CONFIG"; then
			AC_MSG_ERROR([mfsmount build was forced, but fuse library is too old or not installed])
		else
			AC_MSG_ERROR([mfsmount build was forced, but pkg-config not installed])
		fi
	else 
		if test "$enable_mfsmount" != "no"; then
			if test -n "$PKG_CONFIG"; then
				echo "******************************** mfsmount disabled ********************************"
				echo "* fuse library is too old or not installed - mfsmount needs version 2.6 or higher *"
				echo "***********************************************************************************"
			else
				echo "**** mfsmount disabled *****"
				echo "* pkg-config not installed *"
				echo "****************************"
			fi
		fi
	fi
fi
if test "$fuse" != "no" ; then
	case $target_os in
		freebsd6*)
		echo "*************************************************************************"
		echo "* Dynamic linker in FreeBSD 6.x doesn't support symbol versioning.      *"
		echo "* It causes calling wrong functions from libfuse and crashing mfsmount. *"
		echo "* Due to above switching libfuse from dynamic linking to static.        *"
		echo "*************************************************************************"
		FUSE_LIBS="-pthread /usr/local/lib/libfuse.a /usr/local/lib/libiconv.a"
		AC_SUBST(FUSE_LIBS)
		;;
	esac
fi

AC_CHECK_LIB(fuse, fuse_version, [AC_DEFINE([HAVE_FUSE_VERSION],[1],[libfuse has function fuse_version])])

AM_CONDITIONAL([BUILD_MOUNT], [test "$fuse" != "no" -a "$enable_mfsmount" != "no"])

if test "$use_zlib" = "yes"; then

	zlib=no
	AC_CHECK_LIB(z, zlibVersion, [ AC_CHECK_HEADERS(zlib.h,[zlib=yes]) ])

	if test "$zlib" = "no"; then
		ZLIB_LIBS=
		if test "$enable_mfsmaster" != "no" -o "$enable_mfschunkserver" != "no"; then
			AC_MSG_ERROR([zlib development library not found])
		fi
	else
		ZLIB_LIBS=-lz
	fi
	AC_SUBST([ZLIB_LIBS])
fi

test "$prefix" = "NONE" && prefix=$ac_default_prefix
eval DATA_PATH=${localstatedir}/mfs
eval ETC_PATH=${sysconfdir}
# use system /var/run when using system-wide installation prefix
if test "${prefix#/usr}" != "${prefix}" -o "${prefix#/opt}" != "${prefix}"; then
	RUN_PATH=/var/run/mfs
else
	RUN_PATH=${prefix}/var/run/mfs
fi
AC_SUBST([DATA_PATH])
AC_SUBST([ETC_PATH])
AC_SUBST([RUN_PATH])
AC_DEFINE_UNQUOTED([PREFIX], ["${prefix}"], [Installation prefix])
AC_DEFINE_UNQUOTED([ETC_PATH], ["$ETC_PATH"], [Configuration directory])
AC_DEFINE_UNQUOTED([DATA_PATH], ["$DATA_PATH"], [Data directory])
AC_DEFINE_UNQUOTED([RUN_PATH], ["$RUN_PATH"], [PID file directory])

DEFAULT_USER=nobody
DEFAULT_GROUP=
AC_ARG_WITH([default-user],
	[AS_HELP_STRING([--with-default-user=USER], [Choose default user to run daemons as])],
	[DEFAULT_USER=$withval])
AC_ARG_WITH([default-group],
	[AS_HELP_STRING([--with-default-group=GROUP], [Choose default group to run daemons as])],
	[DEFAULT_GROUP=$withval])
AC_DEFINE_UNQUOTED([DEFAULT_USER], ["$DEFAULT_USER"], [Default working user])
AC_DEFINE_UNQUOTED([DEFAULT_GROUP], ["$DEFAULT_GROUP"], [Default working group])
AC_SUBST([DEFAULT_USER])
AC_SUBST([DEFAULT_GROUP])

if test "$enable_lightmfs" = "yes"; then
	PROTO_BASE=65536
	LIGHT_MFS=1
else
	PROTO_BASE=0
	LIGHT_MFS=0
fi
AC_SUBST([PROTO_BASE])
AC_SUBST([LIGHT_MFS])
AC_DEFINE_UNQUOTED([PROTO_BASE], [$PROTO_BASE], [Base value for protocol commands])
AC_DEFINE_UNQUOTED([LIGHT_MFS], [$LIGHT_MFS], [Build light version])

AM_CONDITIONAL([INSTALL_QUOTA_TOOLS], [test "$versmid" -gt "6" -o "$versmaj" -gt "1"])

AC_CONFIG_FILES([Makefile
		doc/Makefile
		mfschunkserver/Makefile
		mfscgi/Makefile
		mfsdata/Makefile
		mfsmaster/Makefile
		mfsmetarestore/Makefile
		mfsmetadump/Makefile
		mfsmetalogger/Makefile
		mfsmount/Makefile
		mfscgi/mfs.cgi
		mfscgi/chart.cgi
		mfsdata/mfschunkserver.cfg
		mfsdata/mfsmaster.cfg
		mfsdata/mfsmetalogger.cfg])
AC_OUTPUT
