
#include <iostream>
#include "libfaster.h"

#define NUMITEMS 10*1000

using namespace std;

pair<int,int> map1(int key, int & input){
	pair<int,int> result (key,input/2);

	return result;
}


pair<int,int> reduce1(int keyA, int &a, int keyB, int &b){
	pair<int,int> result (keyA + keyB/2 ,a+b);

	cerr << result.first << "," << result.second << "  ";

	return result;
}

void printHistogram(const CountKeyMapT<int> & hist ){
	for( auto it = hist.begin(); it != hist.end(); it++){
		cout << it->first << " " << it->second << "\n";
	}
}

int main(int argc, char ** argv){
	// Init Faster Framework
	cout << "Init FastLib" << '\n';
	fastContext fc("local");

	fc.registerFunction((void*) &map1);
	fc.registerFunction((void*) &reduce1);

	fc.startWorkers();

	cout << "Generate Data" << '\n';
	int rawdata[NUMITEMS];
	int rawKeys[NUMITEMS];

	for ( int i = 0; i < NUMITEMS; ++i ){
		rawKeys[i] = rand() % 100;
		rawdata[i] = rand() % 20;
	}

	cout << "Import Data" << '\n';
	indexedFdd <int,int> data(fc, rawKeys, rawdata, NUMITEMS);

	cout << "Key Histogram\n";
	printHistogram(data.countByKey());

	cout << "Process Data" << '\n';
	pair<int,int> result = data.map<int,int>(&map1)->reduce(&reduce1);

	cout << "DONE!" << '\n';

	std::cout << "Resut: " << result.first << ", " << result.second << "\n";

	return 0;
}
