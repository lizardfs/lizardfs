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

#include <string>
#include "common/platform.h"
#include "common/mfserr.h"

#include "client_error_code.h"

lizardfs::detail::lizardfs_error_category lizardfs::detail::lizardfs_error_category::instance_;

std::string lizardfs::detail::lizardfs_error_category::message(int ev) const {
	return lizardfs_error_string(ev);
}

bool lizardfs::detail::lizardfs_error_category::equivalent(
		int code, const std::error_condition &condition) const noexcept {
	if (default_error_condition(code) == condition) {
		return true;
	}

	switch (code) {
	case (int)lizardfs::error::operation_not_permitted:
		return std::make_error_code(std::errc::operation_not_permitted) == condition;
	case (int)lizardfs::error::not_a_directory:
		return std::make_error_code(std::errc::not_a_directory) == condition;
	case (int)lizardfs::error::no_such_file_or_directory:
		return std::make_error_code(std::errc::no_such_file_or_directory) == condition;
	case (int)lizardfs::error::permission_denied:
		return std::make_error_code(std::errc::permission_denied) == condition;
	case (int)lizardfs::error::file_exists:
		return std::make_error_code(std::errc::file_exists) == condition;
	case (int)lizardfs::error::invalid_argument:
		return std::make_error_code(std::errc::invalid_argument) == condition;
	case (int)lizardfs::error::directory_not_empty:
		return std::make_error_code(std::errc::directory_not_empty) == condition;
	case (int)lizardfs::error::no_space_left:
		return std::make_error_code(std::errc::no_space_on_device) == condition;
	case (int)lizardfs::error::io_error:
		return std::make_error_code(std::errc::io_error) == condition;
	case (int)lizardfs::error::read_only_file_system:
		return std::make_error_code(std::errc::read_only_file_system) == condition;
	case (int)lizardfs::error::attribute_not_found:
		return std::make_error_code(std::errc::no_message_available) == condition;
	case (int)lizardfs::error::not_supported:
		return std::make_error_code(std::errc::not_supported) == condition;
	case (int)lizardfs::error::result_out_of_range:
		return std::make_error_code(std::errc::result_out_of_range) == condition;
	case (int)lizardfs::error::no_lock_available:
		return std::make_error_code(std::errc::no_lock_available) == condition;
	case (int)lizardfs::error::filename_too_long:
		return std::make_error_code(std::errc::filename_too_long) == condition;
	case (int)lizardfs::error::file_too_large:
		return std::make_error_code(std::errc::file_too_large) == condition;
	case (int)lizardfs::error::bad_file_descriptor:
		return std::make_error_code(std::errc::bad_file_descriptor) == condition;
	case (int)lizardfs::error::no_message_available:
		return std::make_error_code(std::errc::no_message_available) == condition;
	case (int)lizardfs::error::not_enough_memory:
		return std::make_error_code(std::errc::not_enough_memory) == condition;
	}

	return false;
}

bool lizardfs::detail::lizardfs_error_category::equivalent(const std::error_code &code,
		int condition) const noexcept {
	if (code.category() == *this && code.value() == condition) {
		return true;
	}

	switch (condition) {
	case (int)lizardfs::error::operation_not_permitted:
		return code == std::make_error_condition(std::errc::operation_not_permitted);
	case (int)lizardfs::error::not_a_directory:
		return code == std::make_error_condition(std::errc::not_a_directory);
	case (int)lizardfs::error::no_such_file_or_directory:
		return code == std::make_error_condition(std::errc::no_such_file_or_directory);
	case (int)lizardfs::error::permission_denied:
		return code == std::make_error_condition(std::errc::permission_denied);
	case (int)lizardfs::error::file_exists:
		return code == std::make_error_condition(std::errc::file_exists);
	case (int)lizardfs::error::invalid_argument:
		return code == std::make_error_condition(std::errc::invalid_argument);
	case (int)lizardfs::error::directory_not_empty:
		return code == std::make_error_condition(std::errc::directory_not_empty);
	case (int)lizardfs::error::no_space_left:
		return code == std::make_error_condition(std::errc::no_space_on_device);
	case (int)lizardfs::error::io_error:
		return code == std::make_error_condition(std::errc::io_error);
	case (int)lizardfs::error::read_only_file_system:
		return code == std::make_error_condition(std::errc::read_only_file_system);
	case (int)lizardfs::error::attribute_not_found:
		return code == std::make_error_condition(std::errc::no_message_available);
	case (int)lizardfs::error::not_supported:
		return code == std::make_error_condition(std::errc::not_supported);
	case (int)lizardfs::error::result_out_of_range:
		return code == std::make_error_condition(std::errc::result_out_of_range);
	case (int)lizardfs::error::no_lock_available:
		return code == std::make_error_condition(std::errc::no_lock_available);
	case (int)lizardfs::error::filename_too_long:
		return code == std::make_error_condition(std::errc::filename_too_long);
	case (int)lizardfs::error::file_too_large:
		return code == std::make_error_condition(std::errc::file_too_large);
	case (int)lizardfs::error::bad_file_descriptor:
		return code == std::make_error_condition(std::errc::bad_file_descriptor);
	case (int)lizardfs::error::no_message_available:
		return code == std::make_error_condition(std::errc::no_message_available);
	case (int)lizardfs::error::not_enough_memory:
		return code == std::make_error_condition(std::errc::not_enough_memory);
	}

	return false;
}
