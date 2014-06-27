#pragma once

#include "common/platform.h"

#include <string>

/*! \brief Perform file rotation.
 *
 * \param file - rotate files with given basename.
 * \param storedPreviousCopies - keep that many backup copies.
 */
void rotateFiles(const std::string& file, int storedPreviousCopies);

/*! \brief Perform file rotation and file rename.
 *
 * \param from - rename this file to \e to at end of rotation.
 * \param to - rotate files with given basename.
 * \param storedPreviousCopies - keep that many backup copies.
 */
void rotateFiles(const std::string& from, const std::string& to, int storedPreviousCopies);

