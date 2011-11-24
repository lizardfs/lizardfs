cgibindir = $(CGIDIR)
cgidatadir = $(CGIDIR)
cgiservdir = $(CGISERVDIR)

cgibin_SCRIPTS = \
	chart.cgi \
	mfs.cgi

cgidata_DATA = \
	err.gif \
	index.html \
	logomini.png \
	favicon.ico \
	mfs.css

EXTRA_DIST = mfs.cgi.in chart.cgi.in cgiserv.py.in $(cgibin_SCRIPTS) $(cgidata_DATA)

if INSTALL_CGISERV
cgiserv_SCRIPTS = \
	mfscgiserv

mfscgiserv: cgiserv.py.in
	sed -e "s#@""CGIDIR""@#$(CGIDIR)#" -e "s#@""DATAPATH""@#$(DATA_PATH)#" <cgiserv.py.in >mfscgiserv
endif
