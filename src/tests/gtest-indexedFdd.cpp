#include <algorithm>
#include <vector>

#include "gtest/gtest.h"
#include "libfaster.h"

using namespace std;
using namespace faster;


namespace testfaster{

	TEST(TestIndexedFddCreation, CreateEmptyIFDD){
		int c = 0;
		char ** v = NULL;
		fastContext fc(c, v);
		fc.startWorkers();
		if (!fc.isDriver())
			return;

		indexedFdd<int,int> emptyFDD(fc);
		auto ret = emptyFDD.collect();

		EXPECT_EQ(0, emptyFDD.getSize())
			<< "Empty FDD size() != 0";
		ASSERT_EQ(0, ret.size())
			<< "Collected FDD not empty";
	}


	// Test the creation of a dataset from local data
	TEST(TestIndexedFddCreation, CreateIFDDFromMem){
		const int NUMITEMS = 10*1000;

		int c = 0;
		char ** v = NULL;
		fastContext fc(c, v);
		fc.startWorkers();
		if (!fc.isDriver()){
			return;
		}

		// Create a random data
		vector<int> rawdata(NUMITEMS);
		vector<int> rawkeydata(NUMITEMS);
		vector<pair<int,int>> rawpdata(NUMITEMS);
		for ( size_t i = 0; i < NUMITEMS; ++i ){
			rawdata[i] = (rand() % NUMITEMS);
			rawkeydata[i] = (rand() % NUMITEMS);
			rawpdata[i].first = rawkeydata[i];
			rawpdata[i].second = rawdata[i];
		}


		indexedFdd <int,int> data(fc, rawkeydata.data(), rawdata.data(), rawdata.size());
		auto ret = data.collect();

		EXPECT_EQ(NUMITEMS, data.getSize())
			<< "Empty FDD size() != " << NUMITEMS;
		ASSERT_EQ(NUMITEMS, ret.size())
			<< "Collected FDD size != " << NUMITEMS;

		sort(rawpdata.begin(), rawpdata.end());
		sort(ret.begin(), ret.end());
		for (size_t i = 0; i < NUMITEMS; i++){
			ASSERT_EQ(rawpdata[i].first,ret[i].first) << "Collected FDD differ from original (" << i << ") ";
			ASSERT_EQ(rawpdata[i].second,ret[i].second) << "Collected FDD differ from original (" << i << ") ";
		}
	}

	void update1(int & k, int & a){
		k *= 2;
		a *= 2;
	}

	pair<int,int> map1(const int & k, int & a){
		return make_pair<int,int>(k * 2, a * 2);
	}

	deque<pair<int,int>> flatMap1(int  k, int & a){
		deque<pair<int,int>> r = {{k,a}, {k,a}};
		return r;
	}

	// Data for next tests
	template <typename K, typename T, int NUMITEMS = 10*1000>
		class TestIFDD : public ::testing::Test {
			protected:

				int argc = 0;
				char ** argv = NULL;
				fastContext fc;
				vector<T> localKeyData;
				vector<T> localData;
				indexedFdd<K,T> * data = NULL;

				virtual void SetUp() {
					cerr << "Mockup setup\n";
					fc.registerFunction((void*)&update1, "Update1");
					fc.registerFunction((void*)&map1, "Map1");
					fc.registerFunction((void*)&flatMap1, "FlatMap1");
					fc.startWorkers();
					if (!fc.isDriver()){
						return;
					}

					localKeyData.resize(NUMITEMS);
					localData.resize(NUMITEMS);
					for ( size_t i = 0; i < NUMITEMS; ++i ){
						localKeyData[i] = (rand() % NUMITEMS);
						localData[i] = (rand() % NUMITEMS);
					}
					data = new indexedFdd<K,T>(fc, localKeyData.data(), localData.data(), localData.size());
				}

				virtual void TearDown() {
					cerr << "Mockup teardown\n";
					if (data) delete data;
					//delete localData;
					//delete fc;
				}// */


		};

	typedef TestIFDD<int,int> TestIFDDInt;

	TEST_F(TestIFDDInt, TestMap){
		if (!fc.isDriver()){
			return;
		}
		auto newData = data->map<int,int>(&map1);
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

	TEST_F(TestIFDDInt, TestFlatMap){
		if (!fc.isDriver()){
			return;
		}
		auto newData = data->flatMap<int,int>(&flatMap1);
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


	int main(int argc, char **argv) {

		::testing::InitGoogleTest(&argc, argv);
		return RUN_ALL_TESTS();

	}
};
