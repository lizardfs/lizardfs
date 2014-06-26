#pragma once

#include "common/platform.h"

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

/**
 * Returns version of a metadata file.
 * Throws MetadataCheckException if the file is corrupted, ie. contains wrong header or end marker.
 * \param file -- path to the metadata binary file
 */
uint64_t metadataGetVersion(const std::string& file);

/**
 * Returns version of the first entry in a changelog.
 * Returns 0 in case of any error.
 * \param file -- path to the changelog file
 */
uint64_t changelogGetFirstLogVersion(const std::string& fname);

/**
 * Returns version of the last entry in a changelog.
 * Returns 0 in case of any error.
 * \param file -- path to the changelog file
 */
uint64_t changelogGetLastLogVersion(const std::string& fname);

const uint32_t kDefaultStoredPreviousBackMetaCopies = 1;
const uint32_t kMaxStoredPreviousBackMetaCopies = 99;

