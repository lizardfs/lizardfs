EXTRA_DIST=metadata.mfs mfschunkserver.cfg.in mfsexports.cfg mfsmaster.cfg.in mfshdd.cfg mfsmetalogger.cfg.in

install-data-hook:
	if [ ! -d $(DESTDIR)$(sysconfdir) ]; then \
		$(MKDIR_P) $(DESTDIR)$(sysconfdir) ; \
	fi
if BUILD_CHUNKSERVER
	$(INSTALL_DATA) $(builddir)/mfschunkserver.cfg $(DESTDIR)$(sysconfdir)/mfschunkserver.cfg.dist
	$(INSTALL_DATA) $(srcdir)/mfshdd.cfg $(DESTDIR)$(sysconfdir)/mfshdd.cfg.dist
	if [ ! -d $(DESTDIR)$(DATA_PATH) ]; then \
		$(MKDIR_P) $(DESTDIR)$(DATA_PATH) ; \
		if [ "`id -u`" = "0" ]; then \
			chown $(DEFAULT_USER):$(DEFAULT_GROUP) $(DESTDIR)$(DATA_PATH) ; \
		fi ; \
	fi
endif
if BUILD_MASTER
	$(INSTALL_DATA) $(builddir)/mfsmetalogger.cfg $(DESTDIR)$(sysconfdir)/mfsmetalogger.cfg.dist
	$(INSTALL_DATA) $(builddir)/mfsmaster.cfg $(DESTDIR)$(sysconfdir)/mfsmaster.cfg.dist
	$(INSTALL_DATA) $(builddir)/mfsexports.cfg $(DESTDIR)$(sysconfdir)/mfsexports.cfg.dist
	if [ ! -d $(DESTDIR)$(DATA_PATH) ]; then \
		$(MKDIR_P) $(DESTDIR)$(DATA_PATH) ; \
		if [ "`id -u`" = "0" ]; then \
			chown $(DEFAULT_USER):$(DEFAULT_GROUP) $(DESTDIR)$(DATA_PATH) ; \
		fi ; \
	fi
	$(INSTALL_DATA) $(srcdir)/metadata.mfs $(DESTDIR)$(DATA_PATH)/metadata.mfs.empty
endif
