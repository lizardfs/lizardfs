if BUILD_MASTER
MASTERDIR=mfsmaster mfsmetarestore
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

ACLOCAL_AMFLAGS = $(shell test -d /opt/local/share/aclocal && echo -I/opt/local/share/aclocal)

SUBDIRS=mfsdata doc $(MASTERDIR) $(CHUNKSERVERDIR) $(MOUNTDIR)
