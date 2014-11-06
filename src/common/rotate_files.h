#pragma once

#include "common/platform.h"

#include <string>

/*! \brief Perform file rotation.
 *
 * \param file - rotate files with given basename.
 * \param storedPreviousCopies - keep that many backup copies.
 * \param byNumber - rotate file by given number of positions.
 */
void rotateFiles(const std::string& file, int storedPreviousCopies, int byNumber = 1);
