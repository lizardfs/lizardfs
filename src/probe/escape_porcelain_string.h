#pragma once

#include "common/platform.h"

#include <string>

std::string escapePorcelainString(std::string string) {
	auto replaceAll = [](std::string& str, const std::string& oldText, const std::string& newText) {
		int replaced = 0;
		size_t pos = 0;
		while ((pos = str.find(oldText, pos)) != std::string::npos) {
			str.replace(pos, oldText.length(), newText);
			pos += newText.length();
			++replaced;
		}
		return replaced;
	};
	int replaced = 0;
	replaced += replaceAll(string, "\\", "\\\\");
	replaced += replaceAll(string, "\"", "\\\"");
	if (string.find(' ') != string.npos || string.empty() || replaced != 0) {
		string = '"' + string + '"';
	}
	return string;
}
