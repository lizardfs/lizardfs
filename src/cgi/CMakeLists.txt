configure_file(cgiserv.py.in   lfscgiserv.py)
configure_file(chart.cgi.in    chart.cgi)
configure_file(lfs.cgi.in      lfs.cgi)

set(CGI_FILES err.gif favicon.ico index.html logomini.png lfs.css)
set(CGI_SCRIPTS
    ${CMAKE_CURRENT_BINARY_DIR}/lfs.cgi
    ${CMAKE_CURRENT_BINARY_DIR}/chart.cgi)
set(CGI_SERVER
    ${CMAKE_CURRENT_BINARY_DIR}/lfscgiserv.py)

install(FILES ${CGI_FILES} DESTINATION ${CGI_SUBDIR})
install(PROGRAMS ${CGI_SCRIPTS} DESTINATION ${CGI_SUBDIR})
install(PROGRAMS ${CGI_SERVER} DESTINATION ${SBIN_SUBDIR} RENAME lfscgiserv)
