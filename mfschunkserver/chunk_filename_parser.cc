#include "mfschunkserver/chunk_filename_parser.h"

#include "mfscommon/chunk_type.h"
#include "mfscommon/goal.h"
#include "mfscommon/parser.h"

ChunkFilenameParser::ChunkFilenameParser(const std::string& filename)
	: Parser(filename),
	  chunkType_(ChunkType::getStandardChunkType()),
	  chunkVersion_(0),
	  chunkId_(0),
	  xorPart_(0),
	  xorLevel_(0) {
}

static int isUpperCaseHexDigit(int c) {
	return isdigit(c) || (isxdigit(c) && isupper(c));
}

ChunkFilenameParser::Status ChunkFilenameParser::parseChunkType() {
	bool isParityChunk = (consume("xor_parity_of_") == Parser::OK);
	if (isParityChunk) {
		if (consume("0") == Parser::OK) {
			return ERROR_INVALID_FILENAME;
		}
		if (consume(isdigit) == Parser::OK) {
			if (getLastConsumedCharacterCount() > 2) {
				return ERROR_INVALID_FILENAME;
			}
			xorLevel_ = getDecValue<ChunkType::XorLevel>();
		} else {
			return ERROR_INVALID_FILENAME;
		}
		if (xorLevel_ < kMinXorLevel || xorLevel_ > kMaxXorLevel) {
			return ERROR_INVALID_FILENAME;
		}
		if (consume("_") != Parser::OK) {
			return ERROR_INVALID_FILENAME;
		}
		chunkType_ = ChunkType::getXorParityChunkType(xorLevel_);
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
			xorPart_ = getDecValue<ChunkType::XorPart>();
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
			xorLevel_ = getDecValue<ChunkType::XorLevel>();
		} else {
			return ERROR_INVALID_FILENAME;
		}
		if (xorLevel_ < kMinXorLevel || xorLevel_ > kMaxXorLevel || xorPart_ > xorLevel_) {
			return ERROR_INVALID_FILENAME;
		}
		if (consume("_") != Parser::OK) {
			return ERROR_INVALID_FILENAME;
		}
		chunkType_ = ChunkType::getXorChunkType(xorLevel_, xorPart_);
		return ChunkFilenameParser::OK;
	}

	chunkType_ = ChunkType::getStandardChunkType();
	return ChunkFilenameParser::OK;
}

ChunkFilenameParser::Status ChunkFilenameParser::parse() {
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

	if (consume(".mfs") != Parser::OK) {
		return ERROR_INVALID_FILENAME;
	}

	if (consume(1) == Parser::OK) {
		// trailing characters
		return ERROR_INVALID_FILENAME;
	}

	return OK;
}

ChunkType ChunkFilenameParser::chunkType() const {
	return chunkType_;
}

uint32_t ChunkFilenameParser::chunkVersion() const {
	return chunkVersion_;
}

uint64_t ChunkFilenameParser::chunkId() const {
	return chunkId_;
}

ChunkType::XorPart ChunkFilenameParser::xorPart() const {
	return xorPart_;
}

ChunkType::XorLevel ChunkFilenameParser::xorLevel() const {
	return xorLevel_;
}
