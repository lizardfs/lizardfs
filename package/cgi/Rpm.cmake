set(CPACK_cgi_PACKAGE_DESCRIPTION_SUMMARY "LizardFS CGI client")

if(BUILD_RPM)
  set(CPACK_RPM_cgi_PACKAGE_REQUIRES "filesystem lizardfs-common" PARENT_SCOPE)
  set(CPACK_RPM_cgi_USER_FILELIST
      "%ignore /usr"        #Provided by filesystem
      "%ignore /usr/sbin"
      "%ignore ${DATA_PATH}" #Provided by lizardfs-common
      PARENT_SCOPE)
endif()
if(BUILD_DEB)
  
endif()