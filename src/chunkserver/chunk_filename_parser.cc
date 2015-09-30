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
	  chunkId_(0),
	  xorPart_(0),
	  xorLevel_(0) {
}

static int isUpperCaseHexDigit(int c) {
	return isdigit(c) || (isxdigit(c) && isupper(c));
}

ChunkFilenameParser::Status ChunkFilenameParser::parseChunkType() try {
	bool isParityChunk = (consume("xor_parity_of_") == Parser::OK);
	if (isParityChunk) {
		if (consume("0") == Parser::OK) {
			return ERROR_INVALID_FILENAME;
		}
		if (consume(isdigit) == Parser::OK) {
			if (getLastConsumedCharacterCount() > 2) {
				return ERROR_INVALID_FILENAME;
			}
			xorLevel_ = getDecValue<int>();
		} else {
			return ERROR_INVALID_FILENAME;
		}
		if (xorLevel_ < slice_traits::xors::kMinXorLevel
				|| xorLevel_ > slice_traits::xors::kMaxXorLevel) {
			return ERROR_INVALID_FILENAME;
		}
		if (consume("_") != Parser::OK) {
			return ERROR_INVALID_FILENAME;
		}
		chunkType_ = slice_traits::xors::ChunkPartType(xorLevel_, slice_traits::xors::kXorParityPart);
		return ChunkFilenameParser::OK;
	}

	bool isXorChunk = (consume("xor_") == Parser::OK);
	if (isXorChunk) {
		if (consume("0") == Parser::OK) {
			return ERROR_INVALID_FILENAME;
		}
		if (consume(isdigit) == Parser::OK) {
			if (getLastConsumedCharacterCount() > 2) {
				return ERROR_INVALID_FILENAME;
			}
			xorPart_ = getDecValue<int>();
		} else {
			return ERROR_INVALID_FILENAME;
		}
		if (xorPart_ < 1) {
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
			xorLevel_ = getDecValue<int>();
		} else {
			return ERROR_INVALID_FILENAME;
		}
		if (xorLevel_ < slice_traits::xors::kMinXorLevel
				|| xorLevel_ > slice_traits::xors::kMaxXorLevel
				|| xorPart_ > xorLevel_) {
			return ERROR_INVALID_FILENAME;
		}
		if (consume("_") != Parser::OK) {
			return ERROR_INVALID_FILENAME;
		}
		chunkType_ = slice_traits::xors::ChunkPartType(xorLevel_, xorPart_);
		return ChunkFilenameParser::OK;
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

int ChunkFilenameParser::xorPart() const {
	return xorPart_;
}

int ChunkFilenameParser::xorLevel() const {
	return xorLevel_;
}
