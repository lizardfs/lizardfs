if BUILD_MASTER
MASTERDIR=mfsmaster mfsmetarestore mfsmetadump mfsmetalogger
else
MASTERDIR=
endif

if BUILD_CHUNKSERVER
CHUNKSERVERDIR=mfschunkserver
else
CHUNKSERVERDIR=
endif

if BUILD_MOUNT
MOUNTDIR=mfsmount
else
MOUNTDIR=
endif

if INSTALL_CGI
CGISUBDIR=mfscgi
else
CGISUBDIR=
endif

ACLOCAL_AMFLAGS = -I m4 $(shell test -d /opt/local/share/aclocal && echo -I/opt/local/share/aclocal)

SUBDIRS=mfsdata doc $(MASTERDIR) $(CHUNKSERVERDIR) $(MOUNTDIR) $(CGISUBDIR)

EXTRA_DIST=UPGRADE m4/ax_pthread.m4 debian rpm
