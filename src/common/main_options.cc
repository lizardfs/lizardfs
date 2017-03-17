#include "common/platform.h"
#include "common/main.h"

#include <algorithm>

std::vector<std::string> gExtraArguments;

const std::vector<std::string>& main_get_extra_arguments() {
	return gExtraArguments;
}

bool main_has_extra_argument(std::string name, CaseSensitivity mode) {
	if (mode == CaseSensitivity::kSensitive) {
		std::transform(name.begin(), name.end(), name.begin(), tolower);
	}
	for (auto option : gExtraArguments) {
		if (mode == CaseSensitivity::kSensitive) {
			std::transform(option.begin(), option.end(), option.begin(), tolower);
		}
		if (option == name) {
			return true;
		}
	}
	return false;
}
