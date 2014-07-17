#include <vector>
#include <algorithm>
#include <sstream>

#include "libfaster.h"

#define VECSIZE 100

using namespace std;

// --------- Test FDD creation
//Test Simple > Collect
//Test String > Collect
//Test Vector > Collect
template <typename T>
bool testCreation(fdd<T> * testFDD, fastContext & fc, vector<T> & data){

	testFDD =  new fdd <T> (fc, data);
	vector <T> tv = testFDD->collect();

	if ( data != tv ) {
		return false;
	}

	return true;
}
//Test Pointer > Collect
template <typename T>
bool testCreation(fdd<T*> * testFDD, fastContext & fc, vector<T*> & data, vector<size_t> & dataSizes){

	testFDD =  new fdd <T*> (fc, data.data(), dataSizes.data(), data.size());
	vector <pair<T*,size_t>> tv = testFDD->collect();

	for (int i = 0; i < VECSIZE; ++i){
		for (int j = 0; j < dataSizes[i]; ++j){
			T * p = tv[i].first;
			if ( data[i][j] != tv[i].first[j] ) {
				return false;
			}
		}
	}

	return true;
}
//Test Indexed Simple > Collect
//Test Indexed String > Collect
//Test Indexed Vector > Collect
template <typename K, typename T>
bool testCreation(indexedFdd<K,T> * testFDD, fastContext & fc, vector<K> & keys, vector<T> & data){

	testFDD =  new indexedFdd <K,T> (fc, keys.data(), data.data(), data.size());
	vector <pair<K,T>> tv = testFDD->collect();
	for (int i = 0; i < VECSIZE; ++i){
		cerr << keys[i] << " " << tv[i].first << " ";
		cerr << data[i] << " " << tv[i].second << "\n";
		if ( (data[i] != tv[i].second) || (keys[i] != tv[i].first)) {
		//if ( data[i] != tv[i].second ) {
			return false;
		}
	}

	return true;
}
template <typename K, typename T>
bool testCreation(indexedFdd<K,vector<T>> * testFDD, fastContext & fc, vector<K> & keys, vector<vector<T>> & data){
	testFDD =  new indexedFdd <K,vector<T>> (fc, keys.data(), data.data(), data.size());
	vector <pair<K, vector<T> >> tv = testFDD->collect();
	for (int i = 0; i < VECSIZE; ++i){
		//cerr << keys[i] << " " << tv[i].first << " ";
		//cerr << data[i][0] << " " << tv[i].second[0] << "\n";
		if ( (data[i] != tv[i].second) || (keys[i] != tv[i].first)) {
		//if ( data[i] != tv[i].second ) {
			return false;
		}
	}
	return true;
}
//Test Indexed Pointer > Collect
template <typename K, typename T>
bool testCreation(indexedFdd<K,T*> * testFDD, fastContext & fc, vector<K> & keys, vector<T*> & data, vector<size_t> & dataSizes){

	testFDD =  new indexedFdd <K,T*> (fc, keys.data(), data.data(), dataSizes.data(), data.size());
	vector <tuple<K,T*, size_t>> tv = testFDD->collect();
	for (int i = 0; i < VECSIZE; ++i){
		//if(keys[i] != get<0>(tv[i])) {
		//	return false;
		//}
		for (int j = 0; j < dataSizes[i]; ++j){
			if (data[i][j] != get<1>(tv[i])[j]) {
				return false;
			}
		}
	}

	return true;
}
//Test ReadFile > Collect


// --------- Test Simple Non indexed transformations
//Test Simple Map> Simple
//Test Simple Map> Pointer
//Test Simple Map> String
//Test Simple Map> Vector
//Test Pointer Map> Simple
//Test Pointer Map> Pointer
//Test Pointer Map> String
//Test Pointer Map> Vector
//Test String Map> Simple
//Test String Map> Pointer
//Test String Map> String
//Test String Map> Vector
//Test Vector Map> Simple
//Test Vector Map> Pointer
//Test Vector Map> String
//Test Vector Map> Vector
//Test Simple Reduce> Simple
//Test Pointer Reduce> Pointer
//Test String Reduce> String
//Test Vector Reduce> Vector


// --------- Test Indexed FDDs creation
//Test Simple Map> Indexed Simple
//Test Simple Map> Indexed Pointer
//Test Simple Map> Indexed String
//Test Simple Map> Indexed Vector
//Test Pointer Map> Indexed Simple
//Test Pointer Map> Indexed Pointer
//Test Pointer Map> Indexed String
//Test Pointer Map> Indexed Vector
//Test String Map> Indexed Simple
//Test String Map> Indexed Pointer
//Test String Map> Indexed String
//Test String Map> Indexed Vector
//Test Vector Map> Indexed Simple
//Test Vector Map> Indexed Pointer
//Test Vector Map> Indexed String
//Test Vector Map> Indexed Vector


// --------- Test Simple Indexed transformations
//Test Indexed Simple Map> Indexed Simple
//Test Indexed Simple Map> Indexed Pointer
//Test Indexed Simple Map> Indexed String
//Test Indexed Simple Map> Indexed Vector
//Test Indexed Pointer Map> Indexed Simple
//Test Indexed Pointer Map> Indexed Pointer
//Test Indexed Pointer Map> Indexed String
//Test Indexed Pointer Map> Indexed Vector
//Test Indexed String Map> Indexed Simple
//Test Indexed String Map> Indexed Pointer
//Test Indexed String Map> Indexed String
//Test Indexed String Map> Indexed Vector
//Test Indexed Vector Map> Indexed Simple
//Test Indexed Vector Map> Indexed Pointer
//Test Indexed Vector Map> Indexed String
//Test Indexed Vector Map> Indexed Vector
//Test Indexed Simple Reduce> Indexed Simple
//Test Indexed Pointer Reduce> Indexed Pointer
//Test Indexed String Reduce> Indexed String
//Test Indexed Vector Reduce> Indexed Vector


// --------- Test Non-Indexed FDDs creation
//Test Indexed Simple Map> Simple
//Test Indexed Simple Map> Pointer
//Test Indexed Simple Map> String
//Test Indexed Simple Map> Vector
//Test Indexed Pointer Map> Simple
//Test Indexed Pointer Map> Pointer
//Test Indexed Pointer Map> String
//Test Indexed Pointer Map> Vector
//Test Indexed String Map> Simple
//Test Indexed String Map> Pointer
//Test Indexed String Map> String
//Test Indexed String Map> Vector
//Test Indexed Vector Map> Simple
//Test Indexed Vector Map> Pointer
//Test Indexed Vector Map> String
//Test Indexed Vector Map> Vector



template <typename T>
vector<T> createData(const T & v){
	vector<T> d (VECSIZE, v);
	return d;
}

vector<string> createData(const string & v){
	ostringstream ss;
	vector<string> d (VECSIZE);

	for ( int i = 0; i < VECSIZE; ++i){
		ss.str("");
		ss << v << i;
		d[i] = ss.str();
	}
	return d;
}

template <typename T>
vector<T> createData(){
	vector<T> d (VECSIZE);
	for ( int i = 0; i < VECSIZE; ++i)
		//d[i] = rand() % 10;
		d[i] = i;
	return d;
}

bool assertRecv(int & numOk, int & tot, bool recv){
	if (recv){
		numOk ++;
		cerr << " \033[38;5;29m PASSED\033[0m\n"; 
	}else{
		cerr << " \033[38;5;196m NOT PASSED\033[0m\n";
	}
	tot++;

	return recv;
}
void printResult(int ok, int numOk, int tot){
	if(numOk < tot){ 
		cerr << "\n   \033[38;5;196m NOT PASSED\033[0m";
		cerr << numOk << "/" << tot << "\n"; 
	}else{ 
		cerr << "\n   \033[38;5;29m PASSED\033[0m"; 
		cerr << numOk << "/" << tot << "\n"; 
	}
}

void test(fastContext & fc){
	vector<int> v(2, 1);
	string sv = "Teste";

	vector<size_t> dataSizes(VECSIZE, 2);

	auto keys = createData<int>();
	
	auto dataSimple = createData<int>();
	auto dataPointer = createData(v.data());
	auto dataString = createData(sv);
	auto dataVector = createData(v);

	cerr << "Create Data (Size: " << dataSimple.size() << ")\n"; 
	
	int numOk = 0;
	int tot = 0;
	bool ok = true;

	fdd<int> * testFddS;
	fdd<int*> * testFddP;
	fdd<string> * testFddSt;
	fdd<vector<int>> * testFddV;

	indexedFdd<int,int> * testIFddS;
	indexedFdd<int,int*> * testIFddP;
	indexedFdd<int,string> * testIFddSt;
	indexedFdd<int,vector<int>> * testIFddV;

	cerr << "Simple"; 
	ok &= assertRecv(numOk, tot, testCreation(testFddS,  fc, dataSimple));
	cerr << "Pointer"; 
	ok &= assertRecv(numOk, tot, testCreation(testFddP,  fc, dataPointer, dataSizes));
	cerr << "String"; 
	ok &= assertRecv(numOk, tot, testCreation(testFddSt, fc, dataString));
	cerr << "Vector"; 
	ok &= assertRecv(numOk, tot, testCreation(testFddV,  fc, dataVector));

	cerr << "Indexed"; 
	ok &= assertRecv(numOk, tot, testCreation(testIFddS,  fc, keys, dataSimple));
	cerr << "Indexed Pointer"; 
	ok &= assertRecv(numOk, tot, testCreation(testIFddP,  fc, keys, dataPointer, dataSizes));
	cerr << "Indexed String"; 
	ok &= assertRecv(numOk, tot, testCreation(testIFddSt, fc, keys, dataString));
	cerr << "Indexed Vector"; 
	ok &= assertRecv(numOk, tot, testCreation(testIFddV,  fc, keys, dataVector));

	printResult(ok, numOk, tot);
	if (! ok) return;
}

int main(int argc, char ** argv){

	fastContext fc("local");
	
	fc.startWorkers();


	test(fc);

}
