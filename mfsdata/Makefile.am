EXTRA_DIST=metadata.mfs mfschunkserver.cfg.in mfsmaster.cfg.in mfshdd.cfg

install-data-hook:
	if [ ! -d $(DESTDIR)$(sysconfdir) ]; then \
		$(MKDIR_P) $(DESTDIR)$(sysconfdir) ; \
	fi
if BUILD_CHUNKSERVER
	if [ ! -f $(DESTDIR)$(sysconfdir)/mfschunkserver.cfg ]; then \
		$(INSTALL_DATA) $(builddir)/mfschunkserver.cfg $(DESTDIR)$(sysconfdir) ; \
	fi
	if [ ! -f $(DESTDIR)$(sysconfdir)/mfshdd.cfg ]; then \
		$(INSTALL_DATA) $(srcdir)/mfshdd.cfg $(DESTDIR)$(sysconfdir) ; \
	fi
	if [ ! -d $(DESTDIR)$(RUN_PATH) ]; then \
		$(MKDIR_P) $(DESTDIR)$(RUN_PATH) ; \
		if [ "`id -u`" = "0" ]; then \
			chown $(DEFAULT_USER):$(DEFAULT_GROUP) $(DESTDIR)$(RUN_PATH) ; \
		fi ; \
	fi
	if [ ! -d $(DESTDIR)$(DATA_PATH) ]; then \
		$(MKDIR_P) $(DESTDIR)$(DATA_PATH) ; \
		if [ "`id -u`" = "0" ]; then \
			chown $(DEFAULT_USER):$(DEFAULT_GROUP) $(DESTDIR)$(DATA_PATH) ; \
		fi ; \
	fi
endif
if BUILD_MASTER
	if [ ! -f $(DESTDIR)$(sysconfdir)/mfsmaster.cfg ]; then \
		$(INSTALL_DATA) $(builddir)/mfsmaster.cfg $(DESTDIR)$(sysconfdir) ; \
	fi
	if [ ! -d $(DESTDIR)$(RUN_PATH) ]; then \
		$(MKDIR_P) $(DESTDIR)$(RUN_PATH) ; \
		if [ "`id -u`" = "0" ]; then \
			chown $(DEFAULT_USER):$(DEFAULT_GROUP) $(DESTDIR)$(RUN_PATH) ; \
		fi ; \
	fi
	if [ ! -d $(DESTDIR)$(DATA_PATH) ]; then \
		$(MKDIR_P) $(DESTDIR)$(DATA_PATH) ; \
		if [ "`id -u`" = "0" ]; then \
			chown $(DEFAULT_USER):$(DEFAULT_GROUP) $(DESTDIR)$(DATA_PATH) ; \
		fi ; \
	fi
	if [ ! -f $(DESTDIR)$(DATA_PATH)/metadata.mfs -a ! -f $(DESTDIR)$(DATA_PATH)/metadata.mfs.back ]; then \
		$(INSTALL_DATA) $(srcdir)/metadata.mfs $(DESTDIR)$(DATA_PATH) ; \
	fi
endif
