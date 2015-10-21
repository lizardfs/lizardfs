/*
   Copyright 2015 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/platform.h"
#include "common/media_label.h"

#include <stdexcept>

const int MediaLabelManager::kWildcardHandleValue;
constexpr const char* MediaLabelManager::kWildcard;

const MediaLabel MediaLabel::kWildcard(MediaLabelManager::kWildcardHandleValue);

MediaLabelManager::MediaLabelManager() : next_handle_(1) {
	label_data_.insert({kWildcard, kWildcardHandleValue});
	handle_data_.insert({kWildcardHandleValue, kWildcard});
}

/*! Internal function returning handle to media label string.
 *
 * \param label string representing media label
 * \return handle to media label
 */
MediaLabelManager::HandleValue MediaLabelManager::iGetHandle(const std::string &label) {
	auto ilabel = label_data_.find(label);

	if (ilabel == label_data_.end()) {
		if (next_handle_ == kWildcardHandleValue) {
			throw std::runtime_error("MediaLabelManager::No more space for new label");
		}
		ilabel = label_data_.insert({label, next_handle_}).first;
		try {
			handle_data_.insert({next_handle_, label});
		} catch (...) {
			label_data_.erase(label);
			throw;
		}
		next_handle_++;
	}

	return ilabel->second;
}

/*! Internal function returning label string from handle
 *
 * \param handle handle to label media string
 * \return media label string
 */
std::string MediaLabelManager::iGetLabel(const HandleValue &handle) const {
	auto ihandle = handle_data_.find(handle);

	if (ihandle == handle_data_.end()) {
		throw std::runtime_error("MediaLabelManager::invalid handle");
	}

	return ihandle->second;
}

bool MediaLabelManager::isLabelValid(const std::string &label) {
	const uint32_t maxLength = 32;
	if (label.empty() || label.size() > maxLength) {
		return false;
	}
	for (char c : label) {
		if (!(c == '_' || std::isalnum(c))) {
			return false;
		}
	}
	return true;
}
