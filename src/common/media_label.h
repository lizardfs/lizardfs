#pragma once
#include "common/platform.h"

#include <string>

/// Type used to store labels of media (eg. chunkservers).
typedef std::string MediaLabel;

/// Equals to "_"
/// Media label which has a special meaning as 'any label'
extern const MediaLabel kMediaLabelWildcard;

/// Verifies if the label is valid.
/// Checks if there are only allowed characters used (A-Za-z0-9_)
bool isMediaLabelValid(const MediaLabel& mediaLabel);
