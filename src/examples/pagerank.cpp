#include <iostream>
#include <sstream>
#include "libfaster.h"

#define NUMITEMS 100*1000

using namespace std;
using namespace faster;

const double contribRate = 0.85;

vector<int> split(const string & input){
	vector<int> v;
	int i = 0;
	string token;

	std::istringstream ss( input );
	while (!ss.eof()){
		getline(ss, token, ' ');
		v[i++] = atoi(token.data());
	}

	return v;
}

pair<int, vector<int>> toAList(string & input){
	vector<int>  v = split(input);
	int k = v[0];
	v.erase(v.begin());
	return make_pair( k, v );
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
	auto data = new fdd<string>(fc, "../res/testM.txt");
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
