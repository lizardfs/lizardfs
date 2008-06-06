#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.50)
AC_INIT([MFS], [1.5], [bugs@moosefs.com])
AC_CONFIG_SRCDIR([MFSCommunication.h])
AC_CONFIG_HEADER([conf.h])
AC_CANONICAL_TARGET
AM_INIT_AUTOMAKE([foreign])

# Checks for programs.
AC_PROG_CC
AC_PROG_LN_S

if test "$GCC" = "yes"; then
	CFLAGS="$CFLAGS -W -Wall -Wshadow"
fi

# Checks for libraries.
AC_SEARCH_LIBS([socket], [socket])
AC_SEARCH_LIBS([gethostbyname], [nsl])

# Checks for header files.
AC_HEADER_DIRENT
AC_HEADER_STDC
AC_HEADER_SYS_WAIT
AC_CHECK_HEADERS([arpa/inet.h fcntl.h inttypes.h limits.h netdb.h netinet/in.h stdlib.h string.h sys/socket.h sys/statvfs.h sys/time.h syslog.h unistd.h],, [AC_MSG_ERROR([One of required headers was not found])])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_HEADER_STDBOOL
AC_TYPE_UID_T
AC_TYPE_INT16_T
AC_TYPE_INT32_T
AC_TYPE_INT64_T
AC_TYPE_INT8_T
AC_TYPE_MODE_T
AC_TYPE_OFF_T
AC_TYPE_PID_T
AC_TYPE_SIZE_T
AC_STRUCT_ST_BLOCKS
AC_CHECK_MEMBERS([struct stat.st_rdev])
AC_CHECK_MEMBERS([struct dirent.d_type],,, [#include <dirent.h>])
AC_HEADER_TIME
AC_STRUCT_TM
AC_CHECK_MEMBERS([struct tm.tm_gmtoff],,, [
#ifdef TM_IN_SYS_TIME
#  include <sys/time.h>
#else
#  include <time.h>
#endif])
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

AC_SYS_LARGEFILE

# Checks for library functions.
AC_FUNC_CLOSEDIR_VOID
AC_FUNC_FORK
AC_FUNC_LSTAT
AC_FUNC_MALLOC
AC_FUNC_MEMCMP
AC_FUNC_REALLOC
AC_FUNC_SELECT_ARGTYPES
AC_FUNC_STAT
AC_CHECK_FUNCS([bzero dup2 ftruncate gethostbyname gettimeofday memset mkdir realpath select socket strchr strdup strerror strtol strtoul],, [AC_MSG_ERROR([One of required functions was not found])])

case $target_os in
  *linux*)
	fuse=maybe
	;;
  *solaris*)
	# OpenSolaris has fuse available and is still identified as "solaris"
	fuse=maybe
	;;
  *darwin*)
	fuse=maybe
	# MacFUSE installs to different prefix than pkgconfig
	if test -f /usr/local/lib/pkgconfig/fuse.pc ; then
		PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
		export PKG_CONFIG_PATH
	fi
	;;
  *bsd*)
	fuse=maybe
	AC_DEFINE([NODEFPERM], [1], [Do not use default_permissions for fuse])
	;;
  *)
	fuse=no
	;;
esac

AC_ARG_ENABLE([mfsmaster], [AS_HELP_STRING([--disable-mfsmaster], [Don't build mfsmaster])])
AC_ARG_ENABLE([mfschunkserver], [AS_HELP_STRING([--disable-mfschunkserver], [Don't build mfschunkserver])])
AC_ARG_ENABLE([mfsmount], [AS_HELP_STRING([--disable-mfsmount], [Don't build mfsmount])])

AM_CONDITIONAL([BUILD_MASTER], [test "$enable_mfsmaster" != "no"])
AM_CONDITIONAL([BUILD_CHUNKSERVER], [test "$enable_mfschunkserver" != "no"])
if test "$fuse" != "no" -a "enable_mfsmount" != "no"; then
	PKG_PROG_PKG_CONFIG
	PKG_CHECK_MODULES([FUSE], [fuse],, [fuse=no])
fi
if test "$enable_mfsmount" = "yes" -a "$fuse" = "no"; then
	AC_MSG_ERROR([mfsmount build was forced, but fuse development package is not installed])
fi
AM_CONDITIONAL([BUILD_MOUNT], [test "$fuse" != "no" -a "$enable_mfsmount" != "no"])

test "$prefix" = "NONE" && prefix=$ac_default_prefix
eval DATA_PATH=${localstatedir}/mfs
eval ETC_PATH=${sysconfdir}
# use system /var/run when using system-wide installation prefix
if test "${prefix//usr}" != "${prefix}" -o "${prefix//opt}" != "${prefix}"; then
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
DEFAULT_GROUP=nogroup
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

AC_CONFIG_FILES([Makefile
		doc/Makefile
		mfschunkserver/Makefile
		mfsdata/Makefile
		mfsmaster/Makefile
		mfsmetarestore/Makefile
		mfsmount/Makefile
		mfsdata/mfschunkserver.cfg
		mfsdata/mfsmaster.cfg])
AC_OUTPUT
