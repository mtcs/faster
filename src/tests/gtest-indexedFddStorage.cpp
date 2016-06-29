#include <algorithm>
#include <vector>

#include "gtest/gtest.h"
#include "indexedFddStorage.h"

using namespace std;
using namespace faster;

TEST(testFddStorageCreate, CreateEmpty){
	faster::indexedFddStorage<int,int> storage(0);

	EXPECT_EQ(0, storage.getSize())
		<< "Empty storage has size != 0";
	//EXPECT_EQ(NULL, storage.getData())
	//	<< "Empty storage has internal storage not empty";
}

TEST(testFddStorageCreate, CreateNotEmpty){
	faster::indexedFddStorage<int,int> storage(1024);

	EXPECT_EQ(1024, storage.getSize())
		<< "Storage has size != 1024";
	//EXPECT_STRNE(NULL, storage.getData())
	//	<< "Empty storage has internal storage empty";
}

template <typename K, typename T>
class testFddStorageFunctions : public ::testing::Test {
		protected:

			faster::indexedFddStorage<K,T> storage;
			std::vector<T> rawKeys;
			std::vector<T> rawData;

		virtual void SetUp() {
			rawKeys.resize(1024);
			rawData.resize(1024);
			for ( size_t i = 0; i < rawData.size(); i++ ){
				rawKeys[i] = 2*i;
				rawData[i] = 1024 * rand();
			}
			storage.setData(rawKeys.data(), rawData.data(), 1024);
		}
		virtual void TearDown() { }
};

typedef testFddStorageFunctions<int, int> testFddStorageFunctionsINT;

TEST_F(testFddStorageFunctionsINT, Grow){
	const int size = 16*1024;
	storage.grow(size);
	storage.setSize(size);

	ASSERT_EQ(size, storage.getSize())
		<< "Storage has wrong size";

	for (size_t i = 0; i < rawData.size(); i++){
		ASSERT_EQ(rawKeys[i], storage.getKeys()[i])
			<< "Storage has wrong content";
		ASSERT_EQ(rawData[i], storage[i])
			<< "Storage has wrong content";
	}
}

TEST_F(testFddStorageFunctionsINT, SetData){

	ASSERT_EQ(1024, storage.getSize())
		<< "Storage has wrong size";

	for (size_t i = 0; i < rawData.size(); i++){
		ASSERT_EQ(rawKeys[i], storage.getKeys()[i])
			<< "Storage has wrong content";
		ASSERT_EQ(rawData[i], storage[i])
			<< "Storage has wrong content";
	}
}

TEST_F(testFddStorageFunctionsINT, Insert){
	int key = 1234;
	int val = 1234;

	storage.insert(key,val);

	ASSERT_EQ(1025, storage.getSize())
		<< "Storage has wrong size";

	for (size_t i = 0; i < rawData.size(); i++){
		ASSERT_EQ(rawKeys[i], storage.getKeys()[i])
			<< "Storage has wrong content";
		ASSERT_EQ(rawData[i], storage[i])
			<< "Storage has wrong content";
	}
	ASSERT_EQ(1234, storage[rawData.size()]);
}

namespace {
	int main(int argc, char **argv) {

		::testing::InitGoogleTest(&argc, argv);
		    return RUN_ALL_TESTS();

	}
};
