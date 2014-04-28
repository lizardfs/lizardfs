#pragma once

#include <cstdint>
#include <string>

#include "common/exception.h"

#define METADATA_FILENAME "metadata.mfs"
#define METADATA_BACK_FILENAME METADATA_FILENAME ".back"
#define METADATA_BACK_TMP_FILENAME METADATA_BACK_FILENAME ".tmp"
#define METADATA_EMERGENCY_FILENAME METADATA_FILENAME ".emergency"
#define METADATA_ML_BACK_FILENAME "metadata_ml.mfs.back"

LIZARDFS_CREATE_EXCEPTION_CLASS(MetadataCheckException, Exception);

uint64_t metadata_getversion(const std::string& file);
