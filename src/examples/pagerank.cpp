#include <iostream>
#include "libfaster.h"

#define NUMITEMS 100*1000

using namespace std;
using namespace faster;

const double contribRate = 0.85;

pair<int,vector<int>> toAList(string & input){
	size_t lastPos = 0;
	list<pair<size_t,size_t>> pos;

	//cerr << input << '\n';


	for ( size_t i = 0; i < input.size(); ++i ){
		if ((input[i] == ' ') || (input[i] == '\n')){
			if ((i-lastPos) > 1){
				pos.push_back(make_pair(lastPos, i - lastPos ));
				lastPos = i;
			}
		}
	}
	vector<int> v(pos.size()-1);

	auto it = pos.begin();
	int key = atoi(input.substr(it->first,it->second).data());
	it++;
	for ( size_t i = 0; i < (pos.size()-1) ; ++i){
		v[i] = atoi(input.substr(it->first,it->second).data());
		it++;
		//cerr << v[i-1] << ",";
	}
	//cerr << '\n';

	return make_pair(key,v);
}

pair<int, double> createPR(const int & key, vector<int> & s){
	return make_pair(key, double(1.0L/s.size()));
}

//list<pair<int, double>> givePageRank(const int & key, vector<int> * s, size_t nn, double * pr, size_t npr){
list<pair<int, double>> givePageRank(const int & key, void * sP, size_t nn, void * prP, size_t npr){
	auto s = * (vector<int>*) sP;
	auto pr = * (double*) prP;
	list<pair<int,double>> msgList;

	for ( size_t i = 0; i < s.size(); ++i){
		msgList.push_back(make_pair(s[i], contribRate*pr/s.size()));
	}
	
	return msgList;
}

void getNewPR(const int & key, void * prVP, size_t npr, void * contribVP, size_t numContribs){
	double & pr = * (double*) prVP;
	double * contrib = (double*) contribVP;

	pr *= (1 - contribRate);

	for ( size_t i = 0; i < numContribs; ++i){
		pr += contrib[i];
	}
}



int main(int argc, char ** argv){
	// Init Faster Framework
	cout << "Init FastLib" << '\n';
	fastContext fc("local");
	fc.registerFunction((void*) &toAList);
	fc.registerFunction((void*) &createPR);
	fc.registerFunction((void*) &givePageRank);
	fc.registerFunction((void*) &getNewPR);
	fc.startWorkers();


	cout << "Import Data" << '\n';
	auto data = new fdd<string>(fc, "../res/graph1000.al");
	auto structure = data->map<int, vector<int>>(&toAList);

	cout << "Init Pagerank" << '\n';
	auto pr = structure->map<int, double>(&createPR);
	auto iterationData = structure->cogroup(pr);
	double error = 1;

	cout << "Process Data" << '\n';
	while(error < 0.0001){
		auto contribs = iterationData->flatMapByKey(&givePageRank);
		pr->cogroup(contribs)->updateByKey(&getNewPR);
		//contribs.discard();
	}
	auto result = pr->collect();

	cout << "DONE!" << '\n';
	for ( auto it = result.begin(); it != result.end(); it++){
		std::cout << it->first << " " << it->second << "\n";
	}

	return 0;
}
