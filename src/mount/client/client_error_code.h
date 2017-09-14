/*
   Copyright 2017 Skytechnology sp. z o.o..

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/platform.h"

#include <system_error>

namespace lizardfs {

enum class error {
	success = 0,
	operation_not_permitted,
	not_a_directory,
	no_such_file_or_directory,
	permission_denied,
	file_exists,
	invalid_argument,
	directory_not_empty,
	chunk_lost,
	not_enough_memory,
	index_too_big,
	chunk_locked,
	no_chunk_servers,
	no_such_chunk,
	chunk_is_busy,
	incorrect_register_blob,
	requested_operation_not_completed,
	group_not_registered,
	write_not_started,
	wrong_chunk_version,
	chunk_already_exist,
	no_space_left,
	io_error,
	incorrect_block_number,
	incorrect_file_size,
	incorrect_file_offset,
	cant_connect,
	incorrect_chunk_id,
	disconnected,
	crc_error,
	operation_delayed,
	cant_create_path,
	data_mismatch,
	read_only_file_system,
	quota_exceeded,
	bad_session_id,
	missing_password,
	incorrecet_password,
	attribute_not_found,
	not_supported,
	result_out_of_range,
	operation_timeout,
	bad_metadata_checksum,
	inconsistent_changelog,
	parsing_error,
	metadata_version_mismatch,
	no_lock_available,
	wrong_lock_id,
	operation_not_possible,
	operation_temporarily_not_possible,
	wating_for_completion,
	unknown_error,
	filename_too_long,
	file_too_large,
	bad_file_descriptor,
#if defined(__APPLE__) || defined(__FreeBSD__)
	no_message,
#else
	no_message_available,
#endif
	argument_list_too_long,
};

namespace detail {

class lizardfs_error_category : public std::error_category {
public:
	const char *name() const noexcept override {
		return "lizardfs";
	}

	std::string message(int ev) const override;
	bool equivalent(int code, const std::error_condition &condition) const noexcept override;
	bool equivalent(const std::error_code &code, int condition) const noexcept override;

	static std::error_category &get_instance() {
		return instance_;
	}

private:
	static lizardfs_error_category instance_;
};

}  // detail

inline std::error_condition make_error_condition(error e) {
	return std::error_condition(static_cast<int>(e),
	                            detail::lizardfs_error_category::get_instance());
}

inline std::error_code make_error_code(error e) {
	return std::error_code(static_cast<int>(e),
	                       detail::lizardfs_error_category::get_instance());
}

inline std::error_code make_error_code(int e) {
	return std::error_code(e, detail::lizardfs_error_category::get_instance());
}

}  // lizardfs

namespace std {

template <>
struct is_error_code_enum< ::lizardfs::error> {
	static const bool value = true;
};

template <>
struct is_error_condition_enum< ::lizardfs::error> {
	static const bool value = true;
};

}  // std
