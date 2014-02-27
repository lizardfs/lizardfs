#pragma once

#include <cstdint>
#include <string>

#include "common/exception.h"

LIZARDFS_CREATE_EXCEPTION_CLASS(MetadataCheckException, Exception);

uint64_t metadata_getversion(const std::string& file);
