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

#include "common/platform.h"
#include "chunkserver/chunk_filename_parser.h"

#include "chunkserver/chunk_format.h"
#include "common/chunk_part_type.h"
#include "common/goal.h"
#include "common/parser.h"
#include "common/slice_traits.h"

ChunkFilenameParser::ChunkFilenameParser(const std::string& filename)
	: Parser(filename),
	  chunkFormat_(),
	  chunkType_(),
	  chunkVersion_(0),
	  chunkId_(0) {
}

static int isUpperCaseHexDigit(int c) {
	return isdigit(c) || (isxdigit(c) && isupper(c));
}

ChunkFilenameParser::Status ChunkFilenameParser::parseECChunkType() {
	int data_part_count, parity_part_count, part_index;

	if (consume("0") == Parser::OK) {
		return ERROR_INVALID_FILENAME;
	}
	if (consume(isdigit) == Parser::OK) {
		part_index = getDecValue<int>() - 1;
	} else {
		return ERROR_INVALID_FILENAME;
	}

	if (consume("_of_") != Parser::OK) {
		return ERROR_INVALID_FILENAME;
	}

	if (consume("0") == Parser::OK) {
		return ERROR_INVALID_FILENAME;
	}
	if (consume(isdigit) == Parser::OK) {
		data_part_count = getDecValue<int>();
		if (data_part_count < slice_traits::ec::kMinDataCount ||
		    data_part_count > slice_traits::ec::kMaxDataCount) {
			return ERROR_INVALID_FILENAME;
		}
	} else {
		return ERROR_INVALID_FILENAME;
	}

	if (consume("_") != Parser::OK) {
		return ERROR_INVALID_FILENAME;
	}

	if (consume("0") == Parser::OK) {
		return ERROR_INVALID_FILENAME;
	}
	if (consume(isdigit) == Parser::OK) {
		parity_part_count = getDecValue<int>();
		if (parity_part_count < slice_traits::ec::kMinParityCount ||
		    parity_part_count > slice_traits::ec::kMaxParityCount) {
			return ERROR_INVALID_FILENAME;
		}
	} else {
		return ERROR_INVALID_FILENAME;
	}

	if (consume("_") != Parser::OK) {
		return ERROR_INVALID_FILENAME;
	}

	if (part_index >= (data_part_count + parity_part_count)) {
		return ERROR_INVALID_FILENAME;
	}

	chunkType_ = slice_traits::ec::ChunkPartType(data_part_count, parity_part_count, part_index);
	return ChunkFilenameParser::OK;
}

ChunkFilenameParser::Status ChunkFilenameParser::parseXorChunkType() {
	int xor_level, xor_part;

	bool is_parity = (consume("parity_of_") == Parser::OK);
	if (is_parity) {
		if (consume("0") == Parser::OK) {
			return ERROR_INVALID_FILENAME;
		}
		if (consume(isdigit) == Parser::OK) {
			if (getLastConsumedCharacterCount() > 2) {
				return ERROR_INVALID_FILENAME;
			}
			xor_level = getDecValue<int>();
		} else {
			return ERROR_INVALID_FILENAME;
		}
		if (xor_level < slice_traits::xors::kMinXorLevel ||
		    xor_level > slice_traits::xors::kMaxXorLevel) {
			return ERROR_INVALID_FILENAME;
		}
		if (consume("_") != Parser::OK) {
			return ERROR_INVALID_FILENAME;
		}
		chunkType_ =
		    slice_traits::xors::ChunkPartType(xor_level, slice_traits::xors::kXorParityPart);
		return ChunkFilenameParser::OK;
	}

	if (consume("0") == Parser::OK) {
		return ERROR_INVALID_FILENAME;
	}
	if (consume(isdigit) == Parser::OK) {
		if (getLastConsumedCharacterCount() > 2) {
			return ERROR_INVALID_FILENAME;
		}
		xor_part = getDecValue<int>();
	} else {
		return ERROR_INVALID_FILENAME;
	}
	if (xor_part < 1) {
		return ERROR_INVALID_FILENAME;
	}
	if (consume("_of_") != Parser::OK) {
		return ERROR_INVALID_FILENAME;
	}
	if (consume("0") == Parser::OK) {
		return ERROR_INVALID_FILENAME;
	}
	if (consume(isdigit) == Parser::OK) {
		if (getLastConsumedCharacterCount() > 2) {
			return ERROR_INVALID_FILENAME;
		}
		xor_level = getDecValue<int>();
	} else {
		return ERROR_INVALID_FILENAME;
	}
	if (xor_level < slice_traits::xors::kMinXorLevel ||
	    xor_level > slice_traits::xors::kMaxXorLevel || xor_part > xor_level) {
		return ERROR_INVALID_FILENAME;
	}
	if (consume("_") != Parser::OK) {
		return ERROR_INVALID_FILENAME;
	}
	chunkType_ = slice_traits::xors::ChunkPartType(xor_level, xor_part);
	return ChunkFilenameParser::OK;
}

ChunkFilenameParser::Status ChunkFilenameParser::parseChunkType() try {
	bool is_ec2 = (consume("ec2_") == Parser::OK);

	if (is_ec2) {
		return parseECChunkType();
	}

	bool is_ec = (consume("ec_") == Parser::OK);
	if (is_ec) {
		return parseECChunkType();
	}

	bool is_xor_type = (consume("xor_") == Parser::OK);
	if (is_xor_type) {
		return parseXorChunkType();
	}

	chunkType_ = slice_traits::standard::ChunkPartType();
	return ChunkFilenameParser::OK;
} catch (const std:: invalid_argument &e) {
	return ChunkFilenameParser::ERROR_INVALID_FILENAME;
} catch (const std::out_of_range &e) {
	return ChunkFilenameParser::ERROR_INVALID_FILENAME;
}

ChunkFilenameParser::Status ChunkFilenameParser::parse() try {
	chunkFormat_ = ChunkFormat::INTERLEAVED;

	if (consume("chunk_") != Parser::OK) {
		return ERROR_INVALID_FILENAME;
	}

	if (parseChunkType() != ChunkFilenameParser::OK) {
		return ERROR_INVALID_FILENAME;
	}

	if (consume(isUpperCaseHexDigit) == Parser::OK) {
		if (getLastConsumedCharacterCount() != kChunkIdStringSize) {
			return ERROR_INVALID_FILENAME;
		}
		chunkId_ = getHexValue<uint64_t>();
	} else {
		return ERROR_INVALID_FILENAME;
	}

	if (consume("_") != Parser::OK) {
		return ERROR_INVALID_FILENAME;
	}

	if (consume(isUpperCaseHexDigit) == Parser::OK) {
		if (getLastConsumedCharacterCount() != kChunkVersionStringSize) {
			return ERROR_INVALID_FILENAME;
		}
		chunkVersion_ = getHexValue<uint32_t>();
	} else {
		return ERROR_INVALID_FILENAME;
	}

	if (consume(".liz") != Parser::OK) {
		if (consume(".mfs") == Parser::OK) {
			chunkFormat_ = ChunkFormat::MOOSEFS;
		} else {
			return ERROR_INVALID_FILENAME;
		}
	}

	if (consume(1) == Parser::OK) {
		// trailing characters
		return ERROR_INVALID_FILENAME;
	}

	return OK;
} catch (const std:: invalid_argument &e) {
	return ChunkFilenameParser::ERROR_INVALID_FILENAME;
} catch (const std::out_of_range &e) {
	return ChunkFilenameParser::ERROR_INVALID_FILENAME;
}

ChunkFormat ChunkFilenameParser::chunkFormat() const {
	return chunkFormat_;
}

ChunkPartType ChunkFilenameParser::chunkType() const {
	return chunkType_;
}

uint32_t ChunkFilenameParser::chunkVersion() const {
	return chunkVersion_;
}

uint64_t ChunkFilenameParser::chunkId() const {
	return chunkId_;
}
