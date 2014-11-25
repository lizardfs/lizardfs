if(NOT A2X_BINARY)
  message(WARNING "Program 'a2x' not found, manpages won't be generated")
  return()
endif()

# These manpages should be generated
set(MANPAGES
    lizardfs.7          # not a source
    lizardfs-probe.8
    lfsappendchunks.1
    lfscheckfile.1
    lfsdirinfo.1
    lfsfileinfo.1
    lfsfilerepair.1
    lfsgeteattr.1
    lfsseteattr.1       # not a source
    lfsdeleattr.1       # not a source
    lfsgetgoal.1
    lfssetgoal.1        # not a source
    lfsrgetgoal.1       # not a source
    lfsrsetgoal.1       # not a source
    lfsgettrashtime.1
    lfssettrashtime.1   # not a source
    lfsrgettrashtime.1  # not a source
    lfsrsettrashtime.1  # not a source
    lfsmakesnapshot.1
    lfsmount.1
    lfsrepquota.1
    lfssetquota.1       # not a source
    lfstools.1
    globaliolimits.cfg.5
    iolimits.cfg.5
    lfschunkserver.cfg.5
    lfsexports.cfg.5
    lfsgoals.cfg.5
    lfshdd.cfg.5
    lfsmaster.cfg.5
    lfsmetalogger.cfg.5
    lfstopology.cfg.5
    lizardfs.7
    lfs.7               # not a source
    lfscgiserv.8
    lfschunkserver.8
    lfsmaster.8
    lfsmetalogger.8
    lfsmetarestore.8
)

set(INSTALLED_MANPAGES)
foreach(MANPAGE ${MANPAGES})
  get_filename_component(MANPAGE_SRC ${CMAKE_CURRENT_SOURCE_DIR}/${MANPAGE}.txt ABSOLUTE)
  string(REGEX MATCH "[0-9]$" SECTION_NUMBER ${MANPAGE})
  set(GENERATED_MANPAGE_PATH ${CMAKE_CURRENT_BINARY_DIR}/${MANPAGE})
  if(EXISTS ${MANPAGE_SRC})
    # Generate from the source...
    set(MANPAGE_SRC_SYMLINK ${CMAKE_CURRENT_BINARY_DIR}/${MANPAGE}.txt)
    add_custom_command(OUTPUT ${GENERATED_MANPAGE_PATH}
        COMMAND ln -s -f ${MANPAGE_SRC} ${MANPAGE_SRC_SYMLINK}
        COMMAND a2x -f manpage ${MANPAGE_SRC_SYMLINK}
        DEPENDS ${MANPAGE_SRC})
    # Add this file to the main target's dependencies
    set(GENERATED_MANPAGES ${GENERATED_MANPAGES} ${GENERATED_MANPAGE_PATH})
  endif()
  # Install the generated file
  install(FILES ${GENERATED_MANPAGE_PATH} DESTINATION ${MAN_SUBDIR}/man${SECTION_NUMBER})
endforeach()
add_custom_target(manpages ALL SOURCES ${GENERATED_MANPAGES})
