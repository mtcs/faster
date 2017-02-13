#include <algorithm>
#include <vector>

#include "gtest/gtest.h"
#include "libfaster.h"

using namespace std;
using namespace faster;

namespace testfaster{

	fastContext * fc;

	// Test the creation of a empy dataset
	template <typename T>
	void createEmptyFDDtest(){
		fastContext fc(0, NULL);
		fc.startWorkers();
		if (!fc.isDriver())
			return;

		fdd<T> emptyFDD(fc);
		auto ret = emptyFDD.collect();

		EXPECT_EQ(0, emptyFDD.getSize())
			<< "Empty FDD size() != 0";
		ASSERT_EQ(0, ret.size())
			<< "Collected FDD not empty";
	}
	TEST(TestFddCreation, CreateEmptyFDDChar)   { createEmptyFDDtest<char>(); }
	TEST(TestFddCreation, CreateEmptyFDDInt)    { createEmptyFDDtest<int>();  }
	TEST(TestFddCreation, CreateEmptyFDDLongInt){ createEmptyFDDtest<long int>(); }
	TEST(TestFddCreation, CreateEmptyFDDFloat)  { createEmptyFDDtest<float>();    }
	TEST(TestFddCreation, CreateEmptyFDDDouble) { createEmptyFDDtest<double>();   }
	TEST(TestFddCreation, CreateEmptyFDDString) { createEmptyFDDtest<string>();   }


	template <typename T>
	T createRand(int n, __attribute__((unused))T bla){
		return (rand() % n);
	}
	template <>
	float createRand(int n, __attribute__((unused)) float bla){
		return (rand() / n);
	}
	template <>
	double createRand(int n, __attribute__((unused)) double bla){
		return (rand() / n);
	}
	template <>
	char createRand(__attribute__((unused)) int n, __attribute__((unused)) char bla){
		return (32 + rand() % 90);
	}
	template <>
	string createRand(__attribute__((unused)) int n, __attribute__((unused)) string bla){
		string v;
		v.resize(rand() % 20);
		return v;
	}


	template <typename T>
	testing::AssertionResult assertEq(T a, T b){
		if (a == b)
			return ::testing::AssertionSuccess();
		else
			return ::testing::AssertionFailure();
	}
	template <>
	testing::AssertionResult assertEq(string a, string b){
		if (a.compare(b) == 0)
			return ::testing::AssertionSuccess();
		else
			return ::testing::AssertionFailure();
	}


	// Test the creation of a dataset from local data
	template <typename T, int NUMITEMS = 10*1000>
	void createFDDFromMemTest(){
		fastContext fc(0, NULL);
		fc.startWorkers();
		T gambiarra = {};
		if (!fc.isDriver()){
			return;
		}

		// Create a random data
		vector<T> rawdata(NUMITEMS);
		for ( size_t i = 0; i < NUMITEMS; ++i ){
			rawdata[i] = createRand<T>(NUMITEMS, gambiarra);
		}


		fdd <T> data(fc, rawdata);
		auto ret = data.collect();

		EXPECT_EQ(NUMITEMS, data.getSize())
			<< "Empty FDD size() != " << NUMITEMS;
		ASSERT_EQ(NUMITEMS, ret.size())
			<< "Collected FDD size != " << NUMITEMS;

		sort(rawdata.begin(), rawdata.end());
		sort(ret.begin(), ret.end());
		for (size_t i = 0; i < NUMITEMS; i++){
			ASSERT_TRUE(assertEq<T>(rawdata[i], ret[i]))
				<< "Collected FDD differ from original "
				<< rawdata[i] << " != " << ret[i] << " "
				<< "(" << i << ") ";
		}
	}
	TEST(TestFddCreation, CreateFDDFromMemChar)   { createFDDFromMemTest<char>();    }
	TEST(TestFddCreation, CreateFDDFromMemInt)    { createFDDFromMemTest<int>();     }
	TEST(TestFddCreation, CreateFDDFromMemLongInt){ createFDDFromMemTest<long int>();}
	TEST(TestFddCreation, CreateFDDFromMemFloat)  { createFDDFromMemTest<float>();   }
	TEST(TestFddCreation, CreateFDDFromMemDouble) { createFDDFromMemTest<double>();  }
	TEST(TestFddCreation, CreateFDDFromMemString) { createFDDFromMemTest<string>();  }



	template <typename T>
	void update1(T & a){
		a = a;
	}
	template <>
	void update1(string & a){
		a[0] = a[0];
	}

	template <typename T>
	T map1(T & a){
		return a;
	}

	template <typename T>
	void bulkMap1(T * r, T * a, size_t s){
		for (size_t i = 0; i < s; i++)
			r[i] = a[i];
	}

	template <typename T>
	deque<T> flatMap1(T & a){
		deque<T> r;
		r.push_back(a);
		r.push_back(a);
		return r;
	}
	template <typename T>
	void bulkFlatMap1(T *& r, size_t rs, T * a, size_t as){
		rs = new T[2*as];
		for (size_t i = 0; i < as; i++){
			r[2*i] = a[i];
			r[1+2*i] = a[i];
		}
	}

	template <typename T>
	T reduceFirst(T & a, __attribute__((unused)) T & b){
		return a;
	}
	template <typename T>
	T reduceLast(__attribute__((unused))T & a,  T & b){
		return b;
	}
	template <typename T>
	T bulkReduceFirst(T * a, __attribute__((unused)) size_t s){
		return a[0];
	}
	template <typename T>
	T bulkReduceLast(T * a, size_t s){
		return a[s-1];
	}

	// Data for next tests
	template <typename T, int NUMITEMS = 10*1000>
		class TestFDD : public ::testing::Test {
			protected:



				fastContext fc;
				vector<T> localData;
				fdd<T> * data = NULL;
				T gambiarra = {};

				void collectAndTest(fdd<T> * newData, int propSize){

					vector<T> ret = newData->collect();

					if (propSize > 0){
						ASSERT_EQ(newData->getSize(), propSize * data->getSize())
							<< "Operation does not result size error";
						ASSERT_EQ(ret.size(), propSize * data->getSize())
							<< "Operation does not result size error";
					}
					ASSERT_EQ((size_t)ret.size(), newData->getSize())
						<< "Operation does not result size error";

					if (propSize > 0){
						sort(ret.begin(), ret.end());
						for (size_t i = 0; i < NUMITEMS; i++){
							ASSERT_TRUE(assertEq<T>(localData[i], ret[propSize*i]))
							<< "Collected FDD differ from original "
							<< localData[i] << " != " << ret[propSize*i] << " "
							<< "(" << i << ") ";
						}
					}

				}
				void updateTest(){
					if (!fc.isDriver()){
						return;
					}
					//data->update(&update1);// TODO IMPLEMENT UPDATE FOR FDD
					ASSERT_EQ(false, true) << "update operation not implemented yet";
					vector<T> ret = data->collect();

					ASSERT_EQ(ret.size(), data->getSize()) << "Operation does not result in a same sized dataset";
					sort(ret.begin(), ret.end());

					for (size_t i = 0; i < NUMITEMS; i++){
						ASSERT_TRUE(assertEq<T>(localData[i], ret[i]))
						<< "Collected FDD differ from original "
						<< localData[i] << " != " << ret[i] << " "
						<< "(" << i << ") ";
					}
				}

				void mapTest(){
					if (!fc.isDriver()){
						return;
					}
					fdd<T> * newData = data->map(&map1<T>);
					collectAndTest(newData, 1);
				}
				void bulkMapTest(){
					if (!fc.isDriver()){
						return;
					}
					ASSERT_EQ(false, true) << "bulkMap operation not implemented yet";
					//fdd<T> * newData = data->bulkMap(&bulkMap1<T>);
					//collectAndTest(newData, 1);
				}

				void flatMapTest(){
					if (!fc.isDriver()){
						return;
					}
					fdd<T> * newData = data->flatMap(&flatMap1<T>);
					collectAndTest(newData, 2);
				}

				void bulkFlatMapTest(){
					if (!fc.isDriver()){
						return;
					}
					ASSERT_EQ(false, true) << "bulkFlatMap operation not implemented yet";
					//fdd<T> * newData = data->bulkFlatMap(&bulkFlatMap1<T>);
					//collectAndTest(newData, 2);
				}

				void reduceTest(){
					if (!fc.isDriver()){
						return;
					}
					T first = data->reduce(&reduceFirst<T>);
					ASSERT_TRUE(assertEq<T>(localData[0], first)) <<  "first item does not coincides";

					T last = data->reduce(&reduceLast<T>);
					ASSERT_TRUE(assertEq<T>(localData[NUMITEMS-1], last)) << "last item does not coincides";
				}
				void bulkReduceTest(){
					if (!fc.isDriver()){
						return;
					}
					T first = data->bulkReduce(&bulkReduceFirst<T>);
					ASSERT_TRUE(assertEq<T>(localData[0], first)) <<  "first item does not coincides";

					T last = data->bulkReduce(&bulkReduceLast<T>);
					ASSERT_TRUE(assertEq<T>(localData[NUMITEMS-1], last)) << "last item does not coincides";
				}

				virtual void SetUp() {
					cerr << "Mockup setup\n";
					fc.registerFunction((void*)&update1<T>, "Update1");
					fc.registerFunction((void*)&map1<T>, "Map1");
					fc.registerFunction((void*)&flatMap1<T>, "FlatMap1");
					fc.startWorkers();
					if (!fc.isDriver()){
						return;
					}

					localData.resize(NUMITEMS);
					for ( size_t i = 0; i < NUMITEMS; ++i ){
						localData[i] = createRand(NUMITEMS, gambiarra);
					}
					sort(localData.begin(), localData.end());

					data = new fdd<T>(fc, localData);
				}

				virtual void TearDown() {
					cerr << "Mockup teardown\n";
					if (data) delete data;
					//delete localData;
					//delete fc;
				}// */


		};




	typedef TestFDD<char>     TestFDDChar;
	typedef TestFDD<int>      TestFDDInt;
	typedef TestFDD<long int> TestFDDLongInt;
	typedef TestFDD<float>    TestFDDFloat;
	typedef TestFDD<double>   TestFDDDouble;
	typedef TestFDD<string>   TestFDDString;

	TEST_F(TestFDDChar    , UpdateChar)    { updateTest(); }
	TEST_F(TestFDDInt     , UpdateInt)     { updateTest(); }
	TEST_F(TestFDDLongInt , UpdateLongInt) { updateTest(); }
	TEST_F(TestFDDFloat   , UpdateFloat)   { updateTest(); }
	TEST_F(TestFDDDouble  , UpdateDouble)  { updateTest(); }
	TEST_F(TestFDDString  , UpdateString)  { updateTest(); }

	TEST_F(TestFDDChar    , MapChar)    { mapTest(); }
	TEST_F(TestFDDInt     , MapInt)     { mapTest(); }
	TEST_F(TestFDDLongInt , MapLongInt) { mapTest(); }
	TEST_F(TestFDDFloat   , MapFloat)   { mapTest(); }
	TEST_F(TestFDDDouble  , MapDouble)  { mapTest(); }
	TEST_F(TestFDDString  , MapString)  { mapTest(); }

	TEST_F(TestFDDChar    , BulkMapChar)    { bulkMapTest(); }
	TEST_F(TestFDDInt     , BulkMapInt)     { bulkMapTest(); }
	TEST_F(TestFDDLongInt , BulkMapLongInt) { bulkMapTest(); }
	TEST_F(TestFDDFloat   , BulkMapFloat)   { bulkMapTest(); }
	TEST_F(TestFDDDouble  , BulkMapDouble)  { bulkMapTest(); }
	TEST_F(TestFDDString  , BulkMapString)  { bulkMapTest(); }

	TEST_F(TestFDDChar    , FlatMapChar)    { flatMapTest(); }
	TEST_F(TestFDDInt     , FlatMapInt)     { flatMapTest(); }
	TEST_F(TestFDDLongInt , FlatMapLongInt) { flatMapTest(); }
	TEST_F(TestFDDFloat   , FlatMapFloat)   { flatMapTest(); }
	TEST_F(TestFDDDouble  , FlatMapDouble)  { flatMapTest(); }
	TEST_F(TestFDDString  , FlatMapString)  { flatMapTest(); }

	TEST_F(TestFDDChar    , BulkFlatMapChar)    { bulkFlatMapTest(); }
	TEST_F(TestFDDInt     , BulkFlatMapInt)     { bulkFlatMapTest(); }
	TEST_F(TestFDDLongInt , BulkFlatMapLongInt) { bulkFlatMapTest(); }
	TEST_F(TestFDDFloat   , BulkFlatMapFloat)   { bulkFlatMapTest(); }
	TEST_F(TestFDDDouble  , BulkFlatMapDouble)  { bulkFlatMapTest(); }
	TEST_F(TestFDDString  , BulkFlatMapString)  { bulkFlatMapTest(); }

	TEST_F(TestFDDChar    , ReduceChar)    { reduceTest(); }
	TEST_F(TestFDDInt     , ReduceInt)     { reduceTest(); }
	TEST_F(TestFDDLongInt , ReduceLongInt) { reduceTest(); }
	TEST_F(TestFDDFloat   , ReduceFloat)   { reduceTest(); }
	TEST_F(TestFDDDouble  , ReduceDouble)  { reduceTest(); }
	TEST_F(TestFDDString  , ReduceString)  { reduceTest(); }

	TEST_F(TestFDDChar    , BulkReduceChar)    { bulkReduceTest(); }
	TEST_F(TestFDDInt     , BulkReduceInt)     { bulkReduceTest(); }
	TEST_F(TestFDDLongInt , BulkReduceLongInt) { bulkReduceTest(); }
	TEST_F(TestFDDFloat   , BulkReduceFloat)   { bulkReduceTest(); }
	TEST_F(TestFDDDouble  , BulkReduceDouble)  { bulkReduceTest(); }
	TEST_F(TestFDDString  , BulkReduceString)  { bulkReduceTest(); }


	int main(int argc, char **argv) {

		::testing::InitGoogleTest(&argc, argv);
		return RUN_ALL_TESTS();

	}
};
