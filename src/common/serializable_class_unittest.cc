#include "common/serializable_class.h"

#include <gtest/gtest.h>
#include <tuple>

#include "unittests/inout_pair.h"

TEST(SerializableClassTests, SimpleClass) {
	class Base {};
	SERIALIZABLE_CLASS_BEGIN(SomeClass : public Base)
	SERIALIZABLE_CLASS_BODY(
		SomeClass,
		int   , fieldA,
		short , fieldB,
		long  , fieldC
	)
		SomeClass() = default;
		void myMethod() {
			fieldA_ = 5;
		};
	SERIALIZABLE_CLASS_END;

	SomeClass a;
	(void) a.fieldA_;
	(void) a.fieldB_;
	(void) a.fieldC_;
	a.myMethod();
}

TEST(SerializableClassTests, Serialize) {
	SERIALIZABLE_CLASS_BEGIN(Class)
	SERIALIZABLE_CLASS_BODY(
		Class,
		int   , A,
		short , B,
		long  , C,
		std::string             , D,
		std::vector<std::string>, E
	)
		Class() = default;
		bool operator==(const Class& o) const {
			return std::make_tuple(A_, B_, C_) == std::make_tuple(o.A_, o.B_, o.C_);
		}
		bool operator!=(const Class& o) const {
			return !(*this == o);
		}
	SERIALIZABLE_CLASS_END;

	std::vector<std::string> tmpVector {"kogo", "ma", "ala", "?"};
	Class tmpC {1, 20, 300, "ala ma kota", tmpVector};

	LIZARDFS_DEFINE_INOUT_PAIR(Class, c, tmpC, Class());
	ASSERT_NE(cIn, cOut);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(serialize(buffer, cIn));
	ASSERT_NO_THROW(deserialize(buffer, cOut));
	LIZARDFS_VERIFY_INOUT_PAIR(c);
}
