#include <iostream>
#include "libfaster.h"

#define NUMITEMS (10*10)

using namespace std;
using namespace faster;

/*deque<pair<int,int>> mapByKey1(const int & key, void * aP, size_t sizeA, void * bP, size_t sizeB){
	int * a = (int*) aP;
	int * b = (int*) bP;
	double sum = 0;
	pair<int,int> result (key, 0);
	deque<pair<int,int>> resultl;

	cout << "\033[0;31m";
	cout << key << "\033[0m -  "; cout.flush();

	for ( size_t i = 0; i < sizeA; ++i){
		cout << a[i] << " ";
		sum += (double) a[i]/sizeA;
	}
	for ( size_t i = 0; i < sizeB; ++i){
		cout << b[i] << " ";
		sum += (double) b[i]/sizeB;
	}
	result.second = sum/2;
	cout << "\n";

	resultl.push_back(result);

	return resultl;
}*/

deque<pair<int,int>> mapByKey1(const int & key, deque<void *> * al, deque<void *> * bl){
	double sum = 0;
	pair<int,int> result (key, 0);
	deque<pair<int,int>> resultl;

	cout << "\033[0;31m";
	cout << key << "\033[0m -  "; cout.flush();

	for ( auto it = al->begin(); it != al->end(); it++){
		int a = *(int*) *it;
		cout << a << " ";
		sum += (double) a/al->size();
	}
	for ( auto it = bl->begin(); it != bl->end(); it++){
		int b = *(int*) *it;
		cout << b << " ";
		sum += (double) b/bl->size();
	}
	result.second = sum/2;
	cout << "\n";

	resultl.push_back(result);

	return resultl;
}

void printHistogram(const std::unordered_map<int, size_t> & hist ){
	int sum = 0;
	cout << "\033[0;32m";
	for( auto it = hist.begin(); it != hist.end(); it++){
		cout << it->first << "\t";
	}
	cout << "\033[0m\n";
	for( auto it = hist.begin(); it != hist.end(); it++){
		sum += it->second;
		cout  << it->second << "\t";
	}
	cout << " = " << sum << "\n";
}

int main(int argc, char ** argv){
	// Init Faster Framework
	cout << "Init FastLib" << '\n';
	fastContext fc(argc,argv);

	fc.registerFunction((void*) &mapByKey1);

	fc.startWorkers();

	cout << "Generate Data" << '\n';
	int rawKeys[NUMITEMS];
	int rawdata[NUMITEMS];
	int rawdata2[NUMITEMS];

	for ( int i = 0; i < NUMITEMS; ++i ){
		rawKeys[i] = 1 + rand() % 10;
		rawdata[i] = 1 + rand() % 10;
		rawdata2[i] = 1 + rand() % 10;
	}

	cout << "Import Data" << '\n';
	indexedFdd <int,int> data(fc, rawKeys, rawdata, NUMITEMS);
	indexedFdd <int,int> data2(fc, rawKeys, rawdata2, NUMITEMS);

	cout << "Key Histogram\n";
	printHistogram(data.countByKey());

	cout << "Process Data" << '\n';
	vector<pair<int,int>> result = data.cogroup(&data2)->flatMapByKey<int,int>(&mapByKey1)->collect();

	cout << "DONE!" << '\n';

	std::cout << "Result Size:" << result.size() << "\n";

	cout << "\033[0;31m";
	for ( size_t i = 0; i < result.size(); ++i)
	      cerr << result[i].first << "\t" ;
	cout << "\033[0m\n";
	for ( size_t i = 0; i < result.size(); ++i)
	      cerr << result[i].second << "\t" ;
	cout << "\n";



	return 0;
}
