if BUILD_MASTER
MASTERDIR=mfsmaster mfsmetarestore mfsmetalogger
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

ACLOCAL_AMFLAGS = $(shell test -d /opt/local/share/aclocal && echo -I/opt/local/share/aclocal)

SUBDIRS=mfsdata doc mfscommon $(MASTERDIR) $(CHUNKSERVERDIR) $(MOUNTDIR) $(CGISUBDIR)

EXTRA_DIST=UPGRADE
