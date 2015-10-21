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

#pragma once
#include "common/platform.h"

#include <cstdint>
#include <limits>
#include <string>
#include <unordered_map>

/*! \brief Class responsible for storing media labels.
 *
 * Labels(strings) are registered in hash map and unique
 * 16 bit id is assigned to each string. The class has
 * set of functions to efficiently(constant time)
 * make conversion between labels and ids.
 *
 * There is special wildcard label that is assigned highest possible id.
 * This special value is required to optimize algorithms managing chunk parts.

 * All public functions are static and call corresponding internal functions
 * working on static instance of the class.
 */
class MediaLabelManager {
public:
	typedef uint16_t HandleValue;

	typedef std::unordered_map<std::string, HandleValue> LabelMap;
	typedef std::unordered_map<HandleValue, std::string> HandleMap;

	static const int kWildcardHandleValue = std::numeric_limits<HandleValue>::max();
	constexpr static const char* kWildcard = "_";

public:
	/*! Converts string to unique handle.
	 *
	 * \param label   string representing label
	 * \return handle for label
	 */
	static HandleValue getHandle(const std::string &label) {
		return getInstance().iGetHandle(label);
	}
	/*! Converts handle to string.
	 *
	 * \param handle   handle for label
	 * \return string representing label
	 */
	static std::string getLabel(const HandleValue &handle) {
		return getInstance().iGetLabel(handle);
	}

	/*! Check if string is valid media label.
	 *
	 * \return true if string is valid
	 */
	static bool isLabelValid(const std::string &label);

protected:
	MediaLabelManager();

	HandleValue iGetHandle(const std::string &);
	std::string iGetLabel(const HandleValue &) const;
	void iSetWildcard(const std::string &);
	std::string iGetWildcard() const;

	static MediaLabelManager &getInstance() {
		static MediaLabelManager instance{};
		return instance;
	}

protected:
	LabelMap label_data_;
	HandleMap handle_data_;
	HandleValue next_handle_;
};

/*! \brief Class representing media label.
 *
 * This class uses MediaLabelManager to manage media labels.
 */
class MediaLabel {
public:
	static const MediaLabel kWildcard;

	struct hash {
		typedef MediaLabel argument_type;
		typedef std::size_t result_type;

		result_type operator()(MediaLabel handle) const noexcept {
			return static_cast<result_type>(
			        static_cast<MediaLabelManager::HandleValue>(handle));
		}
	};

public:
	/*! Default constructor creating invalid media label (handle=0). */
	MediaLabel() noexcept : handle_() {
	}

	MediaLabel(const MediaLabel &other) noexcept : handle_(other.handle_) {}
	MediaLabel(MediaLabel &&other) noexcept : handle_(std::move(other.handle_)) {}

	/*! Constructor creating media label from string.
	 *
	 * \param label   string representing label
	 */
	explicit MediaLabel(const std::string &label) {
		handle_ = MediaLabelManager::getHandle(label);
	}

	/*! Constructor creating media label object from handle.
	 *
	 * \param handle   handle to string representing media label
	 */
	explicit MediaLabel(MediaLabelManager::HandleValue handle) noexcept : handle_(handle) {
	}

	MediaLabel &operator=(const MediaLabel &other) noexcept {
		handle_ = other.handle_;
		return *this;
	}

	MediaLabel &operator=(MediaLabel &&other) noexcept {
		handle_ = std::move(other.handle_);
		return *this;
	}

	/*! \brief Conversion to media label string. */
	explicit operator std::string() const {
		return MediaLabelManager::getLabel(handle_);
	}

	/*! \brief Conversion to media label handle. */
	explicit operator MediaLabelManager::HandleValue() const noexcept {
		return handle_;
	}

	bool operator==(const MediaLabel &v) const noexcept {
		return handle_ == v.handle_;
	}

	bool operator!=(const MediaLabel &v) const noexcept {
		return handle_ != v.handle_;
	}

	bool operator<(const MediaLabel &v) const noexcept {
		return handle_ < v.handle_;
	}

	bool operator>(const MediaLabel &v) const noexcept {
		return handle_ > v.handle_;
	}

	bool operator<=(const MediaLabel &v) const noexcept {
		return handle_ <= v.handle_;
	}

	bool operator>=(const MediaLabel &v) const noexcept {
		return handle_ >= v.handle_;
	}

protected:
	MediaLabelManager::HandleValue handle_;
};
