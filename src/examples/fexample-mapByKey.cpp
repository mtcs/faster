#include <iostream>
#include "libfaster.h"

#define NUMITEMS (100*1000)

using namespace std;
using namespace faster;

/*pair<int,int> mapByKey1(const int & key, int * input, size_t size){
	pair<int,int> result (key, 0);

	for ( size_t i = 0; i < size; ++i){
		result.second += input[i];
	}
	result.second /= size;

	return result;
}// */
pair<int,int> mapByKey1(const int & key, list<int *> * input){
	pair<int,int> result (key, 0);

	for ( auto it = input->begin(); it != input->end() ; it++ ){
		result.second += **it;
	}
	result.second /= input->size();

	return result;
}

void printHistogram(const std::unordered_map<int, size_t> & hist ){
	int sum = 0;
	for( auto it = hist.begin(); it != hist.end(); it++){
		cout << it->first << "\t";
	}
	cout << "\n";
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
	int rawdata[NUMITEMS];
	int rawKeys[NUMITEMS];

	for ( int i = 0; i < NUMITEMS; ++i ){
		rawKeys[i] = 1 + rand() % 20;
		rawdata[i] = 1 + rand() % 100;
	}

	cout << "Import Data" << '\n';
	indexedFdd <int,int> data(fc, rawKeys, rawdata, NUMITEMS);

	cout << "Key Histogram\n";
	printHistogram(data.countByKey());
	fc.updateInfo();

	cout << "Process Data" << '\n';
	vector<pair<int,int>> result = data.groupByKey()->mapByKey(&mapByKey1)->collect();
	fc.updateInfo();

	cout << "DONE!" << '\n';

	std::cout << "Result Size:" << result.size() << "\n";
	for ( size_t i = 0; i < result.size(); ++i)
	      cerr << result[i].first << "\t" ;
	cout << "\n";
	for ( size_t i = 0; i < result.size(); ++i)
	      cerr << result[i].second << "\t" ;
	cout << "\n";



	return 0;
}
