if(NOT A2X_BINARY)
  message(WARNING "Program 'a2x' not found, manpages won't be generated")
  return()
endif()

# These manpages should be generated
set(MANPAGES
    lizardfs.7                # not a source
    lizardfs-admin.8
    lizardfs-cgiserver.8
    lizardfs-probe.8          # not a source
    lizardfs-uraft.8
    lizardfs-uraft-helper.8
    lizardfs-uraft.cfg.5
    lizardfs-appendchunks.1
    lizardfs-checkfile.1
    lizardfs-dirinfo.1
    lizardfs-fileinfo.1
    lizardfs-filerepair.1
    lizardfs-geteattr.1
    lizardfs-seteattr.1       # not a source
    lizardfs-deleattr.1       # not a source
    lizardfs-getgoal.1
    lizardfs-setgoal.1        # not a source
    lizardfs-rgetgoal.1       # not a source
    lizardfs-rsetgoal.1       # not a source
    lizardfs-gettrashtime.1
    lizardfs-settrashtime.1   # not a source
    lizardfs-rgettrashtime.1  # not a source
    lizardfs-rsettrashtime.1  # not a source
    lizardfs-makesnapshot.1
    lizardfs-rremove.1
    mfsmount.1
    lizardfs-repquota.1
    lizardfs-setquota.1       # not a source
    lizardfs.1
    globaliolimits.cfg.5
    iolimits.cfg.5
    mfschunkserver.cfg.5
    mfsexports.cfg.5
    mfsgoals.cfg.5
    mfshdd.cfg.5
    mfsmaster.cfg.5
    mfsmetalogger.cfg.5
    mfsmount.cfg.5
    mfstopology.cfg.5
    mfsmetadump.8
    mfsrestoremaster.8
    moosefs.7
    mfs.7                     # not a source
    mfscgiserv.8
    mfschunkserver.8
    mfsmaster.8
    mfsmetalogger.8
    mfsmetarestore.8
)

set(INSTALLED_MANPAGES)
foreach(MANPAGE ${MANPAGES})
  get_filename_component(MANPAGE_SRC ${CMAKE_CURRENT_SOURCE_DIR}/${MANPAGE}.txt ABSOLUTE)
  set(GENERATED_MANPAGE_PATH ${CMAKE_CURRENT_BINARY_DIR}/${MANPAGE})
  if(ENABLE_VERBOSE_ASCIIDOC)
    set(ASCIIDOC_VERBOSE --verbose --keep-artifacts)
  else()
    set(ASCIIDOC_VERBOSE)
  endif()
  if(EXISTS ${MANPAGE_SRC})
    set(MANPAGE_SRC_SYMLINK ${CMAKE_CURRENT_BINARY_DIR}/${MANPAGE}.txt)
    add_custom_command(OUTPUT ${GENERATED_MANPAGE_PATH}
        COMMAND ln -s -f ${MANPAGE_SRC} ${MANPAGE_SRC_SYMLINK}
        COMMAND a2x ${ASCIIDOC_VERBOSE} -L -f manpage ${MANPAGE_SRC_SYMLINK}
        DEPENDS ${MANPAGE_SRC})
    set(GENERATED_MANPAGES ${GENERATED_MANPAGES} ${GENERATED_MANPAGE_PATH})
  endif()
  string(REGEX MATCH "[0-9]$" SECTION_NUMBER ${MANPAGE})
  install(FILES ${GENERATED_MANPAGE_PATH} DESTINATION ${MAN_SUBDIR}/man${SECTION_NUMBER})
endforeach()

# mfsmount3.1 is special, since it should be available even if we do not
# install the fuse2 package with mfsmount.1. Make a copy in the binary
# directory and replace the string "mfsmount" with "mfsmount3".

file(READ ${CMAKE_CURRENT_SOURCE_DIR}/mfsmount.1.txt MFSMOUNT1)
string(REPLACE "mfsmount" "mfsmount3" MFSMOUNT3 ${MFSMOUNT1})
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/mfsmount3.1.txt ${MFSMOUNT3})

add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/mfsmount3.1
    COMMAND a2x ${ASCIIDOC_VERBOSE} -L -f manpage ${CMAKE_CURRENT_BINARY_DIR}/mfsmount3.1.txt
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/mfsmount.1.txt)
set(GENERATED_MANPAGES ${GENERATED_MANPAGES} ${CMAKE_CURRENT_BINARY_DIR}/mfsmount3.1)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/mfsmount3.1 DESTINATION ${MAN_SUBDIR}/man1)

add_custom_target(manpages ALL SOURCES ${GENERATED_MANPAGES})
