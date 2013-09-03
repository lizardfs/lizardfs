#COMPONENT -- component name
#CTL_TYPE -- control type either "postinst" or "prerm"
#PATH -- path to file containing script body
function(set_deb_component_control_extra COMPONENT CTL_TYPE PATH )
  set(DEBIAN_PACKAGE_CONTROL_EXTRA_${CTL_TYPE} "${DEBIAN_PACKAGE_CONTROL_EXTRA_${CTL_TYPE}}-${COMPONENT}.)\n") 
  file(READ ${PATH} BODY)
  set(DEBIAN_PACKAGE_CONTROL_EXTRA_${CTL_TYPE} "${DEBIAN_PACKAGE_CONTROL_EXTRA_${CTL_TYPE}}${BODY}")
  set(DEBIAN_PACKAGE_CONTROL_EXTRA_${CTL_TYPE} "${DEBIAN_PACKAGE_CONTROL_EXTRA_${CTL_TYPE}};;\n" PARENT_SCOPE) 
endfunction()

function(open_deb_component_control_extra CTL_TYPE)
  set(DEBIAN_PACKAGE_CONTROL_EXTRA_${CTL_TYPE} "case \"\$(echo \$0 | grep -o '[-].\\+[.]')\" in\n" PARENT_SCOPE)
endfunction()

function(close_deb_component_control_extra CTL_TYPE)
  set(BODY "*)\necho \"Error code 5ruYrERwj7 : Component unrecognized.\" || false\n;;\nesac\n")
  set(DEBIAN_PACKAGE_CONTROL_EXTRA_${CTL_TYPE} "${DEBIAN_PACKAGE_CONTROL_EXTRA_${CTL_TYPE}}${BODY}")
  file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/${CTL_TYPE}" "${DEBIAN_PACKAGE_CONTROL_EXTRA_${CTL_TYPE}}")
endfunction()

