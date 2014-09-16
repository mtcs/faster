
#include <iostream>
#include "libfaster.h"

#define NUMITEMS (10*1000)

using namespace std;
using namespace faster;

pair<int,int> map1(const int & key, int & input){
	pair<int,int> result (key, input);

	return result;
}


pair<int,int> reduce1(int keyA, int &a, int keyB, int &b){
	pair<int,int> result ((keyA + keyB), (a+b));

	return result;
}

template <typename K>
void printHistogram(const std::unordered_map<K, size_t> & hist ){
	for( auto it = hist.begin(); it != hist.end(); it++){
		cout << it->first << "\t";
	}
	cout << "\n ";
	for( auto it = hist.begin(); it != hist.end(); it++){
		cout  << it->second << "\t";
	}
	cout << "\n ";
}

int main(int argc, char ** argv){
	// Init Faster Framework
	cout << "Init FastLib" << '\n';
	fastContext fc(argc,argv);

	fc.registerFunction((void*) &map1);
	fc.registerFunction((void*) &reduce1);

	fc.startWorkers();

	cout << "Generate Data" << '\n';
	int rawdata[NUMITEMS];
	int rawKeys[NUMITEMS];

	for ( int i = 0; i < NUMITEMS; ++i ){
		rawKeys[i] = 1 + rand() % 10;
		rawdata[i] = 1 + rand() % 100;
	}

	cout << "Import Data" << '\n';
	indexedFdd <int,int> data(fc, rawKeys, rawdata, NUMITEMS);

	cout << "Key Histogram\n";
	printHistogram(data.countByKey());

	cout << "Process Data" << '\n';
	pair<int,int> result = data.map<int,int>(&map1)->reduce(&reduce1);

	cout << "DONE!" << '\n';

	std::cout << "Result: " << result.first/NUMITEMS << ", " << result.second/NUMITEMS << "\n";

	return 0;
}
