configure_file(cgiserv.py.in   mfscgiserv.py)
configure_file(chart.cgi.in    chart.cgi)
configure_file(mfs.cgi.in      mfs.cgi)

set(CGI_FILES err.gif favicon.ico index.html logomini.png mfs.css)
set(CGI_SCRIPTS
    ${CMAKE_CURRENT_BINARY_DIR}/mfs.cgi
    ${CMAKE_CURRENT_BINARY_DIR}/chart.cgi)
set(CGI_SERVER
    ${CMAKE_CURRENT_BINARY_DIR}/mfscgiserv.py)

install(FILES ${CGI_FILES} DESTINATION ${CGI_SUBDIR})
install(PROGRAMS ${CGI_SCRIPTS} DESTINATION ${CGI_SUBDIR})
install(PROGRAMS ${CGI_SERVER} DESTINATION ${SBIN_SUBDIR})
