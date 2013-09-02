configure_file(prerm.in prerm)
configure_file(postinst.in postinst)

#Fix forcing generating package by putting dummy file
install(FILES daemon.touch DESTINATION ${ETC_PATH}/mfs COMPONENT common)
