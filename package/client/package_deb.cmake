set(CPACK_client_PACKAGE_DESCRIPTION_SUMMARY "LizardFS client")

set(CPACK_RPM_client_PACKAGE_REQUIRES "filesystem fuse lizardfs-common" PARENT_SCOPE)

set(CPACK_RPM_client_USER_FILELIST
  "%ignore /usr"        #Provided by filesystem
  "%ignore /usr/sbin"
  "%ignore ${DATA_PATH}" #Provided by lizardfs-common
  PARENT_SCOPE)
  