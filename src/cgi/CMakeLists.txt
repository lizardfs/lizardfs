configure_file(cgiserv.py.in   mfscgiserv)
configure_file(lizardfs-cgiserver.py.in   lizardfs-cgiserver)
configure_file(chart.cgi.in    chart.cgi)
configure_file(mfs.cgi.in      mfs.cgi)

set(CGI_FILES err.gif favicon.ico index.html logomini.png mfs.css)
set(CGI_SCRIPTS
    ${CMAKE_CURRENT_BINARY_DIR}/mfs.cgi
    ${CMAKE_CURRENT_BINARY_DIR}/chart.cgi)
set(CGI_SERVERS
    ${CMAKE_CURRENT_BINARY_DIR}/mfscgiserv
    ${CMAKE_CURRENT_BINARY_DIR}/lizardfs-cgiserver)

install(FILES ${CGI_FILES} DESTINATION ${CGI_SUBDIR})
install(PROGRAMS ${CGI_SCRIPTS} DESTINATION ${CGI_SUBDIR})
install(PROGRAMS ${CGI_SERVERS} DESTINATION ${SBIN_SUBDIR})
