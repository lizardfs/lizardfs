#pragma once

#include "common/platform.h"

#include "common/exception.h"
#include "master/hstring_storage.h"

#include <db.h>
#include <vector>

namespace hstorage {

class BDBStorage : public Storage {
	static_assert(sizeof(db_pgno_t) == 4, "BDBStorage does not support this version of libdb");
	static_assert(sizeof(db_indx_t) == 2, "BDBStorage does not support this version of libdb");
public:
	typedef uint16_t HashType;
	typedef Handle::ValueType ValueType;

	BDBStorage(const ::std::string &path, uint64_t cachesize, int ncache, uint32_t pagesize = 0);

	~BDBStorage();

	bool compare(const Handle &handle, const HString &str) override;
	::std::string get(const Handle &handle) override;
	void copy(Handle &handle, const Handle &other) override;
	void bind(Handle &handle, const HString &str) override;
	void unbind(Handle &handle) override;
	::std::string name() const override;

	static HashType hash(const Handle &handle) {
		return handle.data() >> 48;
	}

private:
	ValueType encode(const DB_HEAP_RID &rid, HashType hash) const;
	DB_HEAP_RID decode(const Handle &handle) const;
	void bind(Handle &handle, const ::std::string &str, HashType hash);

	DB *dbp_;
	std::string path_;

	static constexpr const char *kName = "BDBStorage";
	/*
	 * If first element added to storage happens to have hash == 0,
	 * its handle might look like null. In order to ensure that it never happens,
	 * each used handle is salted with a special value.
	 * */
	static const ValueType kSalt = 1;
};

} //namespace hstring
