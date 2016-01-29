#include "common/platform.h"

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <stdexcept>

#include "master/hstring_bdbstorage.h"

using namespace hstorage;

static const int kGigabyte = (1024 * 1024 * 1024);

BDBStorage::BDBStorage(const std::string &path, uint64_t cachesize, int ncache, uint32_t pagesize)
		: path_(path) {

	int err;

	err = db_create(&dbp_, NULL, 0);
	if (err) {
		throw std::runtime_error("Could not create database");
	}

	if (pagesize != 0) {
		err = dbp_->set_pagesize(dbp_, pagesize);
		if (err) {
			throw std::runtime_error("Could not set pagesize for database");
		}
	}

	err = dbp_->set_cachesize(dbp_, cachesize / kGigabyte, cachesize % kGigabyte , ncache);
	if (err) {
		throw std::runtime_error("Could not set cachesize for database");
	}

	err = dbp_->open(dbp_, NULL, path.c_str(), NULL, DB_HEAP, DB_CREATE | DB_TRUNCATE, 0);
	if (err) {
		throw std::runtime_error("Could not open database");
	}
}

BDBStorage::~BDBStorage() {
	if (dbp_) {
		dbp_->close(dbp_, DB_NOSYNC);
		dbp_ = nullptr;
	}
}

bool BDBStorage::compare(const Handle &handle, const HString &str) {
	if (hash(handle) == static_cast<HashType>(str.hash())) {
		return str == get(handle);
	}
	return false;
}

::std::string BDBStorage::get(const Handle &handle) {
	int err;
	DBT key, data;
	DB_HEAP_RID rid = decode(handle);

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	key.data = &rid;
	key.size = sizeof(rid);
	key.ulen = sizeof(rid);
	key.flags = DB_DBT_USERMEM;

	err = dbp_->get(dbp_, 0, &key, &data, 0);

	if (err) {
		throw std::runtime_error("Getting from database failed");
	}

	return static_cast<const char *>(data.data);
}

void BDBStorage::copy(Handle &handle, const Handle &other) {
	bind(handle, get(other), hash(other));
}

void BDBStorage::bind(Handle &handle, const HString &str) {
	bind(handle, str, str.hash());
}

void BDBStorage::bind(Handle &handle, const std::string &str, HashType hash) {
	int err;
	DBT key, data;
	DB_HEAP_RID rid;
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	memset(&rid, 0, sizeof(rid));

	data.size = (str.size() + 1) * sizeof(char);
	data.data = const_cast<char *>(str.c_str());
	data.flags = DB_DBT_USERMEM;

	key.data = &rid;
	key.size = key.ulen = sizeof(rid);
	key.flags = DB_DBT_USERMEM;

	err = dbp_->put(dbp_, nullptr, &key, &data, DB_APPEND);
	if (err) {
		throw std::runtime_error("Putting to database failed");
	}

	handle.data() = encode(rid, hash);
}

BDBStorage::ValueType BDBStorage::encode(const DB_HEAP_RID &rid, HashType hash) const {
	ValueType ret = static_cast<ValueType>(hash) << 48;
	ret += static_cast<ValueType>(rid.pgno + kSalt) << 16;
	ret += rid.indx;
	return ret;
}

DB_HEAP_RID BDBStorage::decode(const Handle &handle) const {
	/*
	 * Structure of DB_HEAP_RID is
	 * {
	 *    db_pgno_t pgno;
	 *    db_indx_t indx;
	 * }
	 */
	return {static_cast<db_pgno_t>(((handle.data() << 16) >> 32) - kSalt),
			static_cast<db_indx_t>((handle.data() << 48) >> 48)};
}

void BDBStorage::unbind(Handle &handle) {
	DBT key, data;
	DB_HEAP_RID rid = decode(handle);

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	key.data = &rid;
	key.size = sizeof(rid);
	key.ulen = sizeof(rid);
	key.flags = DB_DBT_USERMEM;

	/* Ignore errors, unbind is used in the destructor */
	dbp_->del(dbp_, nullptr, &key, 0);
}

::std::string BDBStorage::name() const {
	return kName;
}
