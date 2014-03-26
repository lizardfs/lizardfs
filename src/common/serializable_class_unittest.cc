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
		long  , fieldC)

		void myMethod() {
			fieldA = 5;
		};
	SERIALIZABLE_CLASS_END;

	SomeClass a;
	(void) a.fieldA;
	(void) a.fieldB;
	(void) a.fieldC;
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
		std::vector<std::string>, E)

		bool operator==(const Class& o) const {
			return std::make_tuple(A, B, C) == std::make_tuple(o.A, o.B, o.C);
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
