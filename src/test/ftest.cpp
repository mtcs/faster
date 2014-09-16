#include <vector>
#include <algorithm>
#include <sstream>

#include "libfaster.h"

#define VECSIZE 1000

using namespace faster;

// Detect difference functions

template <typename T, typename U>
inline bool diff(T & a, U & b){
	return a != b;
}
template <typename T, typename U>
inline bool diff(T & a, U * b){
	return a != b[0];
}
template <typename T, typename U>
inline bool diff(T & a, std::pair<U*,size_t> & b){
	return a != b.first[0];
}
template <typename T, typename U>
inline bool diff(T & a, std::vector<U> & b){
	return a != b[0];
}
template <typename T>
inline bool diff(T & a, std::string & b){
	return char(a) != b[0];
}
template <typename T, typename U>
inline bool diff(std::vector<T> & a, std::vector<U> & b){
	if ( a.size() != b.size() )
		return false;

	for ( size_t i = 0; i < a.size(); ++i){
		return a[i] != b[i];
	}
	return true;
}
inline bool diff(std::string & a, std::string & b){
	if ( a.size() != b.size() )
		return false;

	for ( size_t i = 0; i < a.size(); ++i){
		return a[i] != b[i];
	}
	return true;
}

// Verify result

template <typename T, typename U>
bool verify(std::vector<T> &a, std::vector<U> &b){
	
	if ( a.size() != b.size() )
		return false;

	for (int i = 0; i < VECSIZE; ++i){
		if ( diff( a[i], b[i] ) )
			return false;
	}
	return true;
}

template <typename T, typename U>
bool verify(std::vector<T*> &a, std::vector<std::pair<U*,size_t>> &b, std::vector<size_t> & ds){

	if ( a.size() != b.size() )
		return false;

	for (int i = 0; i < VECSIZE; ++i){
		if ( ds[i] != b[i].second )
			return false;

		for (int j = 0; j < ds[i]; ++j){
			U * p = b[i].first;

			if ( U(a[i][j]) != p[j] ) {
				return false;
			}
		}
	}
	return true;
}
template <typename T, typename U>
bool verify(std::vector<T*> &a, std::vector<U> &b, std::vector<size_t> & ds){

	if ( a.size() != b.size() )
		return false;

	for (int i = 0; i < VECSIZE; ++i){
		if ( ds[i] != b[i].size() )
			return false;

		for (int j = 0; j < ds[i]; ++j){
			if ( a[i][j] != b[i][j] ) {
				return false;
			}
		}
	}
	return true;
}
template <typename K, typename T, typename L, typename U>
bool verify(std::vector<K> &k, std::vector<T> &a, std::vector<std::pair<L,U>> &b){
	
	if ( a.size() != b.size() )
		return false;

	for (int i = 0; i < VECSIZE; ++i){
		if ( (U(a[i]) != b[i].second) || (k[i] != b[i].first)) {
			return false;
		}
	}
	return true;
}
template <typename K, typename T, typename L, typename U>
bool verify(std::vector<K> & k, std::vector<T*> & a, std::vector<size_t> & ds, std::vector<std::tuple<L,U*,size_t>> & b){
	if ( a.size() != b.size() )
		return false;

	for (int i = 0; i < VECSIZE; ++i){
		T * p = std::get<1>(b[i]);

		if ( ds[i] != std::get<2>(b[i])) 
			return false;

		if(k[i] != std::get<0>(b[i])) {
			return false;
		}

		for (int j = 0; j < ds[i]; ++j){
			if (U(a[i][j]) != p[j]) {
				return false;
			}
		}
	}
	return true;
}

// --------- Test FDD creation
//Test Simple > Collect
//Test String > Collect
//Test Vector > Collect
template <typename T>
bool testCreation(fdd<T> *& testFDD, fastContext & fc, std::vector<T> & data){
	testFDD =  new fdd <T> (fc, data);
	std::vector <T> tv = testFDD->collect();

	return verify(data, tv);
}
//Test Pointer > Collect
template <typename T>
bool testCreation(fdd<T*> *& testFDD, fastContext & fc, std::vector<T*> & data, std::vector<size_t> & dataSizes){
	testFDD =  new fdd <T*> (fc, data.data(), dataSizes.data(), data.size());
	std::vector <std::pair<T*,size_t>> tv = testFDD->collect();

	return verify(data, tv, dataSizes);
}
//Test Indexed Simple > Collect
//Test Indexed String > Collect
//Test Indexed Vector > Collect
template <typename K, typename T>
bool testCreation(faster::indexedFdd<K,T> *& testFDD, fastContext & fc, std::vector<K> & keys, std::vector<T> & data){

	testFDD =  new faster::indexedFdd <K,T> (fc, keys.data(), data.data(), data.size());
	std::vector <std::pair<K,T>> tv = testFDD->collect();

	return verify(keys, data, tv);
}
//Test Indexed Pointer > Collect
template <typename K, typename T>
bool testCreation(faster::indexedFdd<K,T*> *& testFDD, fastContext & fc, std::vector<K> & keys, std::vector<T*> & data, std::vector<size_t> & dataSizes){

	testFDD =  new faster::indexedFdd <K,T*> (fc, keys.data(), data.data(), dataSizes.data(), data.size());
	std::vector <std::tuple<K,T*, size_t>> tv = testFDD->collect();

	//return true;
	return verify(keys, data, dataSizes, tv);
}
//Test ReadFile > Collect




// --------- Test Simple Non indexed transformations



// To simple
template <typename T, typename U>
U map1SS(T & a){
	return U(a);
}
// To pointer
template <typename T, typename U>
std::pair<U *,size_t> map1SP(T & a){
	U * r = new U[1];
	r[0] = U(a);
	return std::pair<U*,size_t>(r, 1);
}
// To vector
template <typename T, typename U>
std::vector<U> map1SV(T & a){
	std::vector<U> r(1);
	r[0] = U(a);
	return r;
}
// To String
template <typename T>
std::string map1SSt(T & a){
	std::string r(1,' ');
	r[0] = char (a);
	return r;
}

//Test Simple Map> Simple
//Test Simple Map> Pointer
//Test Simple Map> Vector
template <typename T, typename U>
bool testTransformation(fdd<T> * testFDD, std::vector<U>  data){
	bool ok = true;

	// Transform to another simple
	std::cout << "To Simple\n"; 
	std::vector <U> tv = testFDD->template map<U>(&map1SS<T,U>)->collect();
	ok &= verify(data, tv);
	if (! ok ) std::cout << "\n   \033[38;5;196m NOT PASSED\033[0m\n";

	// Transform to a pointer
	std::cout << "To Pointer\n"; 
	std::vector <std::pair<U*,size_t>> tvP = testFDD->template map<U*>(&map1SP<T,U>)->collect();
	ok &= verify(data, tvP);
	if (! ok ) std::cout << "\n   \033[38;5;196m NOT PASSED\033[0m\n";

	// Transform to pointer
	std::cout << "To Vector\n"; 
	std::vector <std::vector<U>> tvV = testFDD->template map<std::vector<U>>(&map1SV<T,U>)->collect();
	ok &= verify(data, tvV);

	// Transform to string
	std::cout << "To String\n"; 
	std::vector <std::string> tvSt = testFDD->template map<std::string>(&map1SSt<T>)->collect();
	ok &= verify(data, tvSt);

	return ok;
}


// To simple
template <typename T, typename U>
U map1PS(T * a, size_t s UNUSED){
	return (U) a[0];
}
// To pointer
template <typename T, typename U>
std::pair<U *,size_t> map1PP(T * a, size_t s){
	U * r = new U[s];
	for ( size_t i = 0; i < s; ++i ){
		r[i] = U(a[i]);
	}
	return std::pair<U*,size_t>(r, 1);
}
// To vector
template <typename T, typename U>
std::vector<U> map1PV(T * a, size_t s){
	std::vector<U> r(s);
	for ( size_t i = 0; i < s; ++i ){
		r[i] = U(a[i]);
	}
	return r;
}
// To String
template <typename T>
std::string map1PSt(T * a, size_t s){
	std::string r(s, ' ');
	for ( size_t i = 0; i < s; ++i ){
		r[i] = char(a[i]);
	}
	return r;
}
//Test Pointer Map> Simple
//Test Pointer Map> Pointer
//Test Pointer Map> Vector
template <typename T, typename U>
bool testTransformation(fdd<T*> * testFDD, std::vector<U*> & data, std::vector<size_t> & dataSizes){
	bool ok = true;

	// Transform to another simple
	std::cout << "To Simple\n"; 
	std::vector <U> tv = testFDD->template map<U>(&map1PS<T,U>)->collect();
	ok &= verify(data, tv);

	// Transform to a pointer
	std::cout << "To Pointer\n"; 
	std::vector <std::pair<U*,size_t>> tvP = testFDD->template map<U*>(&map1PP<T,U>)->collect();
	ok &= verify(data, tvP, dataSizes);

	// Transform to pointer
	std::cout << "To Vector\n"; 
	std::vector <std::vector<U>> tvV = testFDD->template map<std::vector<U>>(&map1PV<T,U>)->collect();
	ok &= verify(data, tvV, dataSizes);

	// Transform to another simple
	std::cout << "To String\n"; 
	std::vector <std::string> tvSt = testFDD->template map<std::string>(&map1PSt<T>)->collect();
	ok &= verify(data, tvSt);

	return ok;
}


// To simple
template <typename T, typename U>
U map1VS(std::vector<T> & a){
	return U(a[0]);
}
// To pointer
template <typename T, typename U>
std::pair<U *,size_t> map1VP(std::vector<T> & a){
	U * r = new U[a.size()];
	for ( size_t i = 0; i < a.size(); ++i ){
		r[i] = U(a[i]);
	}
	return std::pair<U*,size_t>(r, 1);
}
// To vector
template <typename T, typename U>
std::vector<U> map1VV(std::vector<T> & a){
	std::vector<U> r(a.size());
	for ( size_t i = 0; i < a.size(); ++i ){
		r[i] = U(a[i]);
	}
	return r;
}
// To String
template <typename T>
std::string map1VSt(std::vector<T> & a){
	std::string r(a.size(), ' ');
	for ( size_t i = 0; i < a.size(); ++i ){
		r[i] = char(a[i]);
	}
	return r;
}
//Test Vector Map> Simple
//Test Vector Map> Pointer
//Test Vector Map> Vector
template <typename T, typename U>
bool testTransformation(fdd<std::vector<T>> * testFDD, std::vector<std::vector<U>>  data){
	bool ok = true;

	// Transform to another simple
	std::cout << "To Simple\n"; 
	std::vector <U> tv = testFDD->template map<U>(&map1VS<T,U>)->collect();
	ok &= verify(data, tv);

	// Transform to a pointer
	std::cout << "To Pointer\n"; 
	std::vector <std::pair<U*,size_t>> tvP = testFDD->template map<U*>(&map1VP<T,U>)->collect();
	ok &= verify(data, tvP);

	// Transform to pointer
	std::cout << "To Vector\n"; 
	std::vector <std::vector<U>> tvV = testFDD->template map<std::vector<U>>(&map1VV<T,U>)->collect();
	ok &= verify(data, tvV);

	// Transform to string
	std::cout << "To String\n"; 
	std::vector <std::string> tvSt = testFDD->template map<std::string>(&map1VSt<T>)->collect();
	ok &= verify(data, tvSt);

	return ok;
}
//Test String Map> Simple
//Test String Map> Pointer
//Test String Map> String
//Test String Map> Vector

//Test Simple Reduce> Simple
//Test Pointer Reduce> Pointer
//Test String Reduce> String
//Test Vector Reduce> Vector

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
/*template <typename K, typename T, typename L, typename U>
bool testTransformation(indexedFdd<K,T> * testFDD, fastContext & fc, std::vector<L> & Keys, std::vector<U> & data){

	std::vector <U> tv = testFDD->template map<U>(&map1S<T,U>)->collect();

	return verify(data, tv);
}*/



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
std::vector<T> createData(const T & v){
	std::vector<T> d (VECSIZE, v);
	return d;
}

std::vector<std::string> createData(const std::string & v){
	std::ostringstream ss;
	std::vector<std::string> d (VECSIZE);

	for ( int i = 0; i < VECSIZE; ++i){
		ss.str("");
		ss << v << i;
		d[i] = ss.str();
	}
	return d;
}

template <typename T>
std::vector<T> createData(){
	std::vector<T> d (VECSIZE);
	for ( int i = 0; i < VECSIZE; ++i)
		//d[i] = rand() % 10;
		d[i] = i;
	return d;
}

bool assertRecv(int & numOk, int & tot, bool recv){
	if (recv){
		numOk ++;
		std::cout << " \033[38;5;29m PASSED\033[0m\n"; 
	}else{
		std::cout << " \033[38;5;196m NOT PASSED\033[0m\n";
	}
	tot++;

	return recv;
}
void printResult(int numOk, int tot){
	if(numOk < tot){ 
		std::cout << "\n   \033[38;5;196m NOT PASSED\033[0m ";
		std::cout << numOk << "/" << tot << "\n"; 
	}else{ 
		std::cout << "\n   \033[38;5;29m PASSED\033[0m "; 
		std::cout << numOk << "/" << tot << "\n"; 
	}
}

template <typename T, typename U>
std::vector<U> transformData(std::vector<T> & v){
	std::vector<U> d (v.size());
	for ( size_t i = 0; i < v.size(); ++i ){
		d[i] = U(v[i]);
	}
	return d;
}
template <typename T>
std::vector<std::string> transformDataSt(std::vector<T> & v){
	std::vector<std::string> d (v.size());
	for ( size_t i = 0; i < v.size(); ++i ){
		d[i] = char(v[i]);
	}
	return d;
}


void test(fastContext & fc){
	std::vector<int> v(2, 1);
	std::string sv = "Teste";
	std::string svK = "Key";

	std::vector<size_t> dataSizes(VECSIZE, 2);

	auto keys = createData<int>();
	auto stringKeys = createData(svK);
	
	auto dataSimple = createData<int>();
	auto dataSimpleF = transformData<int, float>(dataSimple);
	auto dataSimpleSt = transformDataSt<int>(dataSimple);
	auto dataPointer = createData(v.data());
	auto dataString = createData(sv);
	auto dataVector = createData(v);

	std::cout << "Create Data (Size: " << dataSimple.size() << ")\n"; 
	
	int numOk = 0;
	int tot = 0;
	bool ok = true;

	fdd<int> * testFddS = NULL;
	fdd<int*> * testFddP = NULL;
	fdd<std::string> * testFddSt = NULL;
	fdd<std::vector<int>> * testFddV = NULL;

	indexedFdd<int, int> * testIFddS = NULL;
	indexedFdd<int, int*> * testIFddP = NULL;
	indexedFdd<int, std::vector<int>> * testIFddV = NULL;
	indexedFdd<int, std::string> * testIFddSt = NULL;
	indexedFdd<std::string, std::string> * testIFddStSt = NULL;

	std::cout << "Simple"; 
	ok &= assertRecv(numOk, tot, testCreation(testFddS,  fc, dataSimple));
	std::cout << "Pointer"; 
	ok &= assertRecv(numOk, tot, testCreation(testFddP,  fc, dataPointer, dataSizes));
	std::cout << "Vector"; 
	ok &= assertRecv(numOk, tot, testCreation(testFddV,  fc, dataVector));
	std::cout << "String"; 
	ok &= assertRecv(numOk, tot, testCreation(testFddSt, fc, dataString));

	printResult(numOk, tot);
	if (! ok) return;

	std::cout << "Indexed"; 
	ok &= assertRecv(numOk, tot, testCreation(testIFddS,  fc, keys, dataSimple));
	std::cout << "Indexed Pointer"; 
	ok &= assertRecv(numOk, tot, testCreation(testIFddP,  fc, keys, dataPointer, dataSizes));
	std::cout << "Indexed Vector"; 
	ok &= assertRecv(numOk, tot, testCreation(testIFddV,  fc, keys, dataVector));
	std::cout << "Indexed String"; 
	ok &= assertRecv(numOk, tot, testCreation(testIFddSt, fc, keys, dataString));
	std::cout << "Indexed (String, String)"; 
	ok &= assertRecv(numOk, tot, testCreation(testIFddStSt, fc, stringKeys, dataString));

	printResult(numOk, tot);
	if (! ok) return;

	std::cout << "Simple"; 
	ok &= assertRecv(numOk, tot, testTransformation(testFddS,  dataSimpleF));
	//std::cout << "Pointer"; 
	//ok &= assertRecv(numOk, tot, testTransformation(testFddP, dataPointer, dataSizes));
	//std::cout << "Vector"; 
	//ok &= assertRecv(numOk, tot, testTransformation(testFddV,  dataVector));
	//std::cout << "String"; 
	//ok &= assertRecv(numOk, tot, testTransformation(testFddSt,  dataString));


	printResult(numOk, tot);
	if (! ok) return;
}

int main(int argc UNUSED, char ** argv UNUSED){

	fastContext fc(argc,argv);

	fc.registerFunction((void*) &map1SS<int,float>);
	fc.registerFunction((void*) &map1SP<int,float>);
	fc.registerFunction((void*) &map1SV<int,float>);
	fc.registerFunction((void*) &map1SSt<int>);

	fc.registerFunction((void*) &map1PS<int,float>);
	fc.registerFunction((void*) &map1PP<int,float>);
	fc.registerFunction((void*) &map1PV<int,float>);
	fc.registerFunction((void*) &map1PSt<int>);
	
	fc.registerFunction((void*) &map1VS<int,float>);
	fc.registerFunction((void*) &map1VP<int,float>);
	fc.registerFunction((void*) &map1VV<int,float>);
	fc.registerFunction((void*) &map1VSt<int>);
	
	//fc.registerFunction((void*) &map1StS<float>);
	//fc.registerFunction((void*) &map1StP<float*>);
	//fc.registerFunction((void*) &map1StV<std::vector<float>>);
	//fc.registerFunction((void*) &map1StSt);
	
	fc.startWorkers();


	test(fc);

}
