#include "config.h"
#include "probe/options.h"

Options::Options(const std::vector<std::string>& expectedOptions,
		const std::vector<std::string>& argv) {
	// Set expected options
	for (const auto& option : expectedOptions) {
		options_[option] = false;
	}

	// Set some to true using provided argv
	for (const std::string& arg : argv) {
		if (arg.substr(0, 2) == "--") {
			if (!isOptionExpected(arg)) {
				throw ParseError("Unexpected option " + arg);
			}
			options_[arg] = true;
		} else {
			arguments_.push_back(arg);
		}
	}
}
