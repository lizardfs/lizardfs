#TARGET_VARIABLE_NAME -- variable name to which content will be appended
#SOURCE_FILE_PATH -- path to file which content will be appended to variable

function(append_file_to_var TARGET_VARIABLE_NAME  SOURCE_FILE_PATH)
  file(READ ${SOURCE_FILE_PATH} BODY)
  set(${TARGET_VARIABLE_NAME} "${TARGET_VARIABLE_NAME}${BODY}" PARENT_SCOPE)
endfunction()
