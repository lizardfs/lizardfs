cgibindir = $(CGIDIR)
cgidatadir = $(CGIDIR)
cgiservdir = $(CGISERVDIR)

cgibin_DATA = \
	cgi/chart.cgi \
	cgi/mfs.cgi

cgidata_DATA = \
	config.ini \
	err.gif \
	index.html \
	logomini.png \
	mfs.css

EXTRA_DIST = cgiserv.py.in $(cgibin_DATA) $(cgidata_DATA)

if INSTALL_CGISERV
cgiserv_SCRIPTS = \
	mfscgiserv

mfscgiserv: cgiserv.py.in
	sed -e "s#@""CGIDIR""@#$(CGIDIR)#" <cgiserv.py.in >mfscgiserv
endif
