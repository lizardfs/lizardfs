#include "common/platform.h"
#include "common/media_label.h"

const MediaLabel kMediaLabelWildcard = "_";

bool isMediaLabelValid(const MediaLabel& mediaLabel) {
	for (char c : mediaLabel) {
		if (!(c == '_'
				|| (c >= 'a' && c <= 'z')
				|| (c >= 'A' && c <= 'Z')
				|| (c >= '0' && c <= '9'))) {
			return false;
		}
	}
	return !mediaLabel.empty();
}
