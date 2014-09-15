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

	cerr << input << '\n';


	for ( size_t i = 0; i < input.size(); ++i ){
		if (input[i] == ' '){
			if ((i-lastPos) > 1){
				pos.push_back(make_pair(lastPos+1, i));
				//cerr <<  "P";
			}
			//cerr << " ";
			lastPos = i;
		}
		//else{cerr << input[i];}
	}
				//cerr <<  "P";
	pos.push_back(make_pair(lastPos+1, input.size()));
	//cerr << '\n';
	vector<int> v(pos.size()-1);

	auto it = pos.begin();
	int key = atoi(input.substr(it->first,it->second).data());
	it++;
	//cerr << key << " ";
	for ( size_t i = 0; i < (pos.size()-1) ; ++i){
		v[i] = atoi(input.substr(it->first,it->second).data());
		it++;
		//cerr << v[i] << ",";
	}
	//cerr << '\n';

	return make_pair(key,v);
}

pair<int, double> createPR(const int & key, vector<int> & s){
	return make_pair(key, 1);
}

//list<pair<int, double>> givePageRank(const int & key, vector<int> * s, size_t nn, double * pr, size_t npr){
list<pair<int, double>> givePageRank(const int & key, void * sP, size_t nn, void * prP, size_t npr){
	if (( sP == NULL ) || ( nn == 0)) {
		cerr << "!!!! "<< key << " #" << nn << "!!!! \n"; 
	}
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

	return oldPR - pr;
}

double sum( double & a, double & b){
	return a+b;
}



int main(int argc, char ** argv){
	// Init Faster Framework
	cout << "Init FastLib" << '\n';
	fastContext fc("local");
	fc.registerFunction((void*) &toAList);
	fc.registerFunction((void*) &createPR);
	fc.registerFunction((void*) &givePageRank);
	fc.registerFunction((void*) &getNewPR);
	fc.registerFunction((void*) &sum);
	fc.startWorkers();


	cout << "Import Data" << '\n';
	auto data = new fdd<string>(fc, "../res/graph100.al");
	auto structure = data->map<int, vector<int>>(&toAList)->cache();

	cout << "Init Pagerank" << '\n';
	auto pr = structure->map<int, double>(&createPR)->cache();
	auto iterationData = structure->cogroup(pr);
	double error = 1;

	cout << "Process Data" << '\n';
	int i = 0;
	while(error > 0.0001){
		cout << " Iteration " << i++ << '\n';
		auto contribs = iterationData->flatMapByKey(&givePageRank);
		error = pr->cogroup(contribs)->mapByKey(&getNewPR)->reduce(&sum)/structure->getSize();
		cout << "  Error " << error << '\n';
		//contribs.discard();
	}
	auto result = pr->collect();

	cout << "Sorting" << '\n';
	sort(result.begin(), result.end(), [](const pair<int,double> a, const pair<int,double> b){ return a.first < b.first; });

	cout << "DONE! S:" << result.size() << '\n';
	for ( auto it = result.begin(); it != result.end(); it++){
		std::cout << it->first << " " << it->second << "\n";
	}

	return 0;
}
