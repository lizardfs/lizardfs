/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

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

#include <utility>

/**
 * A class which calls the given function in its destructor.
 * Usable when one wants do do some cleanup in a function which has multiple ways of exiting.
 */
template <typename Function>
class LambdaGuard {
public:
	explicit LambdaGuard(Function f) : valid_(true), f_(std::move(f)) {}

	LambdaGuard(LambdaGuard&) = delete; // to make using these objects safer

	LambdaGuard(LambdaGuard&& other) : valid_(other.valid_), f_(std::move(other.f_)) {
		other.valid_ = false;
	}

	~LambdaGuard() {
		if (valid_) {
			f_();
		}
	}

private:
	/// true iff we should call \p f_ in the destructor
	bool valid_;

	/// function to be called in the destructor
	Function f_;
};

/**
 * Creates a LambdaGuard object.
 * Usage example: see the unit test.
 * \param f some function-like object
 */
template <typename Function>
LambdaGuard<Function> makeLambdaGuard(Function f) {
	return LambdaGuard<Function>(std::move(f));
}
