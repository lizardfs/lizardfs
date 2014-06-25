#pragma once
#include "config.h"

#include <string>

template <class C>
std::string toString(C value) {
	return std::to_string(value);
}
