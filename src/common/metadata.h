#pragma once

#include "common/platform.h"

#include <cstdint>
#include <string>
#include <memory>

#include "common/exception.h"
#include "common/lockfile.h"

extern const char kMetadataFilename[];
extern const char kMetadataTmpFilename[];
extern const char kMetadataMlFilename[];
extern const char kMetadataMlTmpFilename[];
extern const char kMetadataEmergencyFilename[];
extern const char kChangelogFilename[];
extern const char kChangelogTmpFilename[];
extern const char kChangelogMlFilename[];
extern const char kChangelogMlTmpFilename[];
extern const char kSessionsFilename[];
extern const char kSessionsTmpFilename[];
extern const char kSessionsMlFilename[];
extern const char kSessionsMlTmpFilename[];

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

/**
 * Rename changelog files from old to new version
 * from <name>.X.lfs to <name>.lfs.X
 * Used only once - after upgrade from version before 1.6.29
 * \param name -- changelog name before first dot
 */
void changelogsMigrateFrom_1_6_29(const std::string& fname);

extern std::unique_ptr<Lockfile> gMetadataLockfile;

