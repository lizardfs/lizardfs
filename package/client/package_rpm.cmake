set(COMPONENT_NAME "chunkserver")

set(CPACK_${COMPONENT_NAME}_PACKAGE_DESCRIPTION_SUMMARY "LizardFS client")

set(CPACK_RPM_${COMPONENT_NAME}_PACKAGE_REQUIRES "filesystem fuse lizardfs-common" PARENT_SCOPE)

set(CPACK_RPM_${COMPONENT_NAME}_USER_FILELIST
  "%ignore /usr"        #Provided by filesystem
  "%ignore /usr/sbin"
  "%ignore ${DATA_PATH}" #Provided by lizardfs-common
  PARENT_SCOPE)
