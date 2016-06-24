#include <algorithm>
#include <vector>

#include "gtest/gtest.h"
#include "libfaster.h"

using namespace std;
using namespace faster;

fastContext * fc;

// Test the creation of a empy dataset
TEST(TestFddCreation, CreateEmptyFDD){
	fastContext fc(0, NULL);
	fc.startWorkers();
	if (!fc.isDriver())
		return;

	fdd<int> emptyFDD(fc);
	auto ret = emptyFDD.collect();

	EXPECT_EQ(0, emptyFDD.getSize())
		<< "Empty FDD size() != 0";
	ASSERT_EQ(0, ret.size())
		<< "Collected FDD not empty";
}

// Test the creation of a dataset from local data
TEST(TestFddCreation, CreateFDDFromMem){
	const int NUMITEMS = 10*1000;

	fastContext fc(0, NULL);
	fc.startWorkers();
	if (!fc.isDriver()){
		return;
	}

	// Create a random data
	vector<int> rawdata(NUMITEMS);
	for ( size_t i = 0; i < NUMITEMS; ++i ){
		rawdata[i] = (rand() % NUMITEMS);
	}


	fdd <int> data(fc, rawdata.data(), rawdata.size());
	auto ret = data.collect();

	EXPECT_EQ(NUMITEMS, data.getSize())
		<< "Empty FDD size() != " << NUMITEMS;
	ASSERT_EQ(NUMITEMS, ret.size())
		<< "Collected FDD size != " << NUMITEMS;

	sort(rawdata.begin(), rawdata.end());
	sort(ret.begin(), ret.end());
	for (size_t i = 0; i < NUMITEMS; i++){
		ASSERT_EQ(rawdata[i],ret[i]) << "Collected FDD differ from original (" << i << ") ";
	}
}

void update1(int & a){
	a *= 2;
}

int map1(int & a){
	return a * 2;
}

deque<int> flatMap1(int & a){
	deque<int> r = {a, a};
	return r;
}

// Data for next tests
template <typename T, int NUMITEMS = 10*1000>
class TestFDD : public ::testing::Test {
	protected:

		fastContext fc;
		vector<T> localData;
		fdd<T> * data;

		virtual void SetUp() {
			cerr << "Mockup setup\n";
			fc.registerFunction((void*)&update1, "Update1");
			fc.registerFunction((void*)&map1, "Map1");
			fc.registerFunction((void*)&flatMap1, "FlatMap1");
			fc.startWorkers();
			if (!fc.isDriver()){
				return;
			}

			localData.resize(NUMITEMS);
			for ( size_t i = 0; i < NUMITEMS; ++i ){
				localData[i] = (rand() % NUMITEMS);
			}
			data = new fdd<T>(fc, localData);
		}

		virtual void TearDown() {
			cerr << "Mockup teardown\n";
			delete data;
			//delete localData;
			//delete fc;
		}// */


};

/*TEST_F(FasterFDD, TestUpdate){
	if (!fc->isDriver()){
		return;
	}
	data->update(&update1);
	auto ret = data->collect();

	EXPECT_EQ(ret.size(), data->getSize()) << "Map does not result in a same sized dataset";

	for (size_t i = 0; i < NUMITEMS; i++){
		ASSERT_EQ(localData[0][i], 2*ret[i]) << "Collected FDD differ from original (" << i << ") ";
	}
}// */

typedef TestFDD<int> TestFDDInt;

TEST_F(TestFDDInt, TestMap){
	if (!fc.isDriver()){
		return;
	}
	auto newData = data->map<int>(&map1);
	auto ret = newData->collect();

	EXPECT_EQ(newData->getSize(), data->getSize())
		<< "Map does not result in a same sized dataset";
	EXPECT_EQ(ret.size(), data->getSize())
		<< "Map does not result in a same sized dataset";
	ASSERT_EQ(ret.size(), newData->getSize())
		<< "Map does not result in a same sized dataset";

	//for (size_t i = 0; i < NUMITEMS; i++){
	//	ASSERT_EQ(localData[0][i], 2*ret[i])
	//		<< "Collected FDD differ from original (" << i << ") ";
	//}
}// */

TEST_F(TestFDDInt, TestFlatMap){
	if (!fc.isDriver()){
		return;
	}
	auto newData = data->flatMap<int>(&flatMap1);
	auto ret = newData->collect();

	EXPECT_EQ(newData->getSize(), 2 * data->getSize())
		<< "Map does not result in a same sized dataset";
	EXPECT_EQ(ret.size(), 2 * data->getSize())
		<< "Map does not result in a same sized dataset";
	ASSERT_EQ(ret.size(), newData->getSize())
		<< "Map does not result in a same sized dataset";
	//for (size_t i = 0; i < NUMITEMS; i++){
	//	ASSERT_EQ(localData[0][2*i], ret[i])
	//		<< "Collected FDD differ from original (" << i << ") ";
	//}
}// */

namespace {
	int main(int argc, char **argv) {

		::testing::InitGoogleTest(&argc, argv);
		    return RUN_ALL_TESTS();

	}
};
