#pragma once

#include "config.h"

#include <cstdint>
#include <string>

#include "common/exception.h"

extern const char kMetadataFilename[];
extern const char kMetadataTmpFilename[];
extern const char kMetadataBackFilename[];
extern const char kMetadataEmergencyFilename[];
extern const char kMetadataMlBackFilename[];
extern const char kChangelogFilename[];

LIZARDFS_CREATE_EXCEPTION_CLASS(MetadataCheckException, Exception);

uint64_t metadata_getversion(const std::string& file);

const uint32_t kDefaultStoredPreviousBackMetaCopies = 1;
const uint32_t kMaxStoredPreviousBackMetaCopies = 99;

