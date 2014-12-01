#pragma once
#include "common/platform.h"

#include <fstream>
#include <sstream>
#include <memory>
#include <mutex>
#include <vector>

#ifdef LIZARDFS_ENABLE_DEBUG_LOG
# define DEBUG_LOG(TAG) debugLog(TAG, __FILE__, __func__, __LINE__)
# define DEBUG_LOGF(TAG, format, ...) debugLogf(TAG, __FILE__, __PRETTY_FUNCTION__, __LINE__, \
		format, __VA_ARGS__)
# define DEBUG_LOGFV(TAG, format, ap) debugLogfv(TAG, __FILE__, __PRETTY_FUNCTION__, __LINE__, \
		format, ap)
#else
// define DEBUG_LOG in the way that it generates no code nor any compiler errors/warnings
# define DEBUG_LOG(TAG) if (false) *static_cast<std::ofstream*>(nullptr) << 0
# define DEBUG_LOGF(TAG, format, ...) /**/
# define DEBUG_LOGFV(TAG, format, ...) /**/
#endif

/*! \brief Provide means for logging to multiple files at once.
 */
class DebugLog {
public:
	typedef std::vector<std::string> Paths;

	/*! \brief Create new DebugLog object.
	 *
	 * Newly created DebugLog object will have opened given number of files,
	 * those files will be closed when DebugLog object ceases to exists.
	 *
	 * \param paths - set of path names for requested log output files.
	 */
	DebugLog(const Paths& paths);

	/*! \brief Flush remaining data from buffer.
	 */
	virtual ~DebugLog() {
		if (buffer_ != nullptr) {
			*buffer_ << std::endl;
			flush();
		}
	}

	DebugLog(const DebugLog&) = delete;

	DebugLog(DebugLog&& other)
			: buffer_(std::move(other.buffer_)),
			  streams_(std::move(other.streams_)) {
		other.buffer_ = nullptr;
	}

	/*! \brief Append given value to set of opened log files.
	 *
	 * \param val - value to append to opened log files.
	 * \return Self.
	 */
	template<typename T>
	DebugLog& operator<<(T&& val) {
		*buffer_ << std::forward<T>(val);
		return *this;
	}

	/*! \brief Apply given manipulator to set of opened streams.
	 *
	 * \param manip - manipulator to be applied to all opened streams in DebugLog object.
	 * \return Self.
	 */
	DebugLog& operator<<(std::ostream& (*manip)(std::ostream&)) {
		*buffer_ << manip;
		return *this;
	}

	/*! \brief Configure DebugLog
	 *
	 * \param configuration - a string in the form: prefix1:file1,prefix2:file2...
	 */
	static void setConfiguration(std::string configuration);

	/*! \brief Get the current configuration of the DebugLog
	 *
	 * \return current configuration, ie. a string in the form: prefix1:file1,prefix2:file2...
	 */
	static std::string getConfiguration();

private:
	typedef std::unique_ptr<std::ofstream> Stream;

	void flush() {
		for (Stream& o : streams_) {
			*o << buffer_->str();
		}
		buffer_->str(std::string());
	}

	/*! \brief Guards access to configurationString_
	 */
	static std::mutex configurationMutex_;

	/*! \brief String with DebugLog's configuration
	 *
	 * The format is: prefix1:file1,prefix2:file2,prefix3:file3,...,prefixN:fileN
	 */
	static std::string configurationString_;

	std::unique_ptr<std::stringstream> buffer_;
	std::vector<Stream> streams_;
};

/*! \brief Write log message to file.
 *
 * \param tag - select log file based on tag.
 * \param originFile - source file where given debugLog is called.
 * \param originFunction - function where given debugLog is called.
 * \param originLine - line number where given debugLog is called.
 * \return File stream proprer for given tag.
 */
DebugLog debugLog(const std::string& tag, const char* originFile,
		const char* originFunction, int originLine);

/*! \brief Write printf style message to file.
 *
 * \param tag - select log file based on tag.
 * \param originFile - source file where given debugLog is called.
 * \param originFunction - function where given debugLog is called.
 * \param originLine - line number where given debugLog is called.
 * \param format - printf() style format string.
 */
void debugLogf(const std::string& tag, const char* originFile,
		const char* originFunction, int originLine, const char* format, ...);
void debugLogfv(const std::string& tag, const char* originFile,
		const char* originFunction, int originLine, const char* format, va_list ap);
