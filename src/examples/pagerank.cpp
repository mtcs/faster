#include <iostream>
#include <algorithm>
#include "libfaster.h"

#define NUMITEMS 100*1000

using namespace std;
using namespace faster;

const double dumpingFactor = 0.85;

pair<int,vector<int>> toAList(string & input){
	long int lastPos = -1;
	list<pair<size_t,size_t>> pos;


	for ( size_t i = 0; i < input.size(); ++i ){
		if (input[i] == ' '){
			if ((i-lastPos) > 1){
				pos.push_back(make_pair(lastPos+1, i));
			}
			lastPos = i;
		}
	}
	pos.push_back(make_pair(lastPos+1, input.size()));
	vector<int> v(pos.size()-1);

	auto it = pos.begin();
	int key = atoi(input.substr(it->first,it->second).data());
	it++;
	for ( size_t i = 0; i < (pos.size()-1) ; ++i){
		v[i] = atoi(input.substr(it->first,it->second).data());
		it++;
	}

	return make_pair(key,v);
}

pair<int, double> createPR(const int & key, vector<int> & s){
	return make_pair(key, 1);
}

//list<pair<int, double>> givePageRank(const int & key, vector<int> * s, size_t nn, double * pr, size_t npr){
list<pair<int, double>> givePageRank(const int & key, void * sP, size_t nn, void * prP, size_t npr){
	auto s = * (vector<int>*) sP;
	auto pr = * (double*) prP;
	list<pair<int,double>> msgList;

	for ( size_t i = 0; i < s.size(); ++i){
		msgList.push_back(make_pair(s[i], dumpingFactor*pr/s.size()));
	}
	
	return msgList;
}

double getNewPR(const int & key, void * prVP, size_t npr, void * contribVP, size_t numContribs){
	//cerr << key << " ";
	double & pr = * (double*) prVP;
	double * contrib = (double*) contribVP;
	double oldPR = pr;
	double sum = 0;


	for ( size_t i = 0; i < numContribs; ++i){
		sum += contrib[i];
	}
	pr = (1 - dumpingFactor) + dumpingFactor * sum;

	return abs(oldPR - pr);
}

double sum( double & a, double & b){
	return a+b;
}



int main(int argc, char ** argv){
	// Init Faster Framework
	fastContext fc(argc, argv);
	fc.registerFunction((void*) &toAList);
	fc.registerFunction((void*) &createPR);
	fc.registerFunction((void*) &givePageRank);
	fc.registerFunction((void*) &getNewPR);
	fc.registerFunction((void*) &sum);
	fc.startWorkers();


	cerr << "Import Data" << '\n';
	auto data = new fdd<string>(fc, argv[1]);
	auto structure = data->map<int, vector<int>>(&toAList)->cache();

	cerr << "Init Pagerank" << '\n';
	auto pr = structure->map<int, double>(&createPR)->cache();
	auto iterationData = structure->cogroup(pr);
	double error = 1;

	cerr << "Process Data" << '\n';
	int i = 0;
	while( error >= 0.01){
		cerr << "Iteration " << i++ << '\n';
		auto contribs = iterationData->flatMapByKey(&givePageRank);
		error = pr->cogroup(contribs)->mapByKey(&getNewPR)->reduce(&sum) / structure->getSize();
		cerr << "Error " << error << '\n';
		//contribs.discard();
	}
	auto result = pr->collect();

	cerr << "Sorting" << '\n';
	sort(result.begin(), result.end(), [](const pair<int,double> a, const pair<int,double> b){ return a.first < b.first; });

	cerr << "PageRank in " << structure->getSize() << " node graph in "<< i << " iterations! S:" << result.size() << " (error: " << error <<  ") \n";
	for ( auto it = result.begin(); it != result.end(); it++){
		std::cout << it->first << " " << it->second << "\n";
	}

	return 0;
}
