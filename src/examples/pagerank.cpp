#include <iostream>
#include <algorithm>
#include "libfaster.h"

#define NUMITEMS 100*1000

using namespace std;
using namespace faster;

const double dumpingFactor = 0.85;

using std::chrono::system_clock;
using std::chrono::duration_cast;
using std::chrono::milliseconds;

pair<int,vector<int>> toAList(string & input){
	long int lastPos = -1;
	list<pair<size_t,size_t>> pos;


	for ( size_t i = 0; i < input.size(); ++i ){
		if ( (input[i] == ' ') && ( i < (input.size()-1) ) ){
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
/*list<pair<int, double>> givePageRank(const int & key, void * sP, size_t nn, void * prP, size_t npr){
	auto s = * (vector<int>*) sP;
	auto pr = * (double*) prP;
	list<pair<int,double>> msgList;

	for ( size_t i = 0; i < s.size(); ++i){
		msgList.push_back(make_pair(s[i], dumpingFactor*pr/s.size()));
	}
	
	return msgList;
}*/
list<pair<int, double>> givePageRank(const int & key, list<void *> * sl, list<void *> * prl){
	auto & s = * (vector<int>*) * sl->begin();
	auto & pr = * (double*) * prl->begin();
	list<pair<int,double>> msgList;
	double contrib = dumpingFactor * pr / s.size();


	for ( size_t i = 0; i < s.size(); ++i){
		msgList.push_back(make_pair(s[i], contrib));
	}
	
	return msgList;
}
pair<int, double> combine(const int & key, list<double *> * prl){
	pair<int,double> r;

	r.first = key;
		
	//cerr << key << ":" << prl.size() << " ";
	for ( auto it = prl->begin(); it != prl->end(); it++){
		r.second += **it;
	}

	return r;
}

/*double getNewPR(const int & key, void * prVP, size_t npr, void * contribVP, size_t numContribs){
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
}*/
double getNewPR(const int & key, list<void *> * prL, list<void *> * contribL){
	//cerr << key << " ";
	double & pr = * (double*) * prL->begin();
	double oldPR = pr;
	double sum = 0;


	for( auto it = contribL->begin(); it != contribL->end(); it++){
		double contrib = *(double*) *it;
		sum += contrib;
	}
	//pr = (1 - dumpingFactor) + dumpingFactor * sum;
	pr = (1 - dumpingFactor) + sum;

	return abs(oldPR - pr);
}

double maxError( double & a, double & b){
	return max(a,b);
}



int main(int argc, char ** argv){
	// Init Faster Framework

	auto start = system_clock::now();

	fastContext fc(argc, argv);
	fc.registerFunction((void*) &toAList);
	fc.registerFunction((void*) &createPR);
	fc.registerFunction((void*) &givePageRank);
	fc.registerFunction((void*) &combine);
	fc.registerFunction((void*) &getNewPR);
	fc.registerFunction((void*) &maxError);
	fc.startWorkers();


	if ( (argc > 2) && (argv[2][0] == '1') ){
		cerr << "Calibrate Performance\n";
		fc.calibrate();
		fc.updateInfo();
	}

	cerr << "Import Data\n";
	auto data = new fdd<string>(fc, argv[1]);

	cerr << "Convert to Adjacency List\n";
	auto structure = data->map<int, vector<int>>(&toAList)->cache();
	fc.updateInfo();

	cerr << "Init Pagerank\n";
	auto pr = structure->map<int, double>(&createPR)->cache();
	auto iterationData = structure->cogroup(pr);
	double error = 1000;
	fc.updateInfo();

	cerr << "Process Data\n";
	int i = 0;
	while( error >= 1){
		cerr << "Iteration " << i++ << "\n" ;
		auto contribs = iterationData->flatMapByKey(&givePageRank);
		fc.updateInfo();

		auto combContribs = contribs->mapByKey(&combine);
		fc.updateInfo();

		error = pr->cogroup(combContribs)->mapByKey(&getNewPR)->reduce(&maxError);
		//error = pr->cogroup(contribs)->mapByKey(&getNewPR)->reduce(&maxError);
		cerr << contribs->getSize() << " (" << combContribs->getSize() << ") messages. Error " << error << '\n';
		//cerr << contribs->getSize() << " messages. Error " << error << '\n';

		fc.updateInfo();
	}
	auto result = pr->collect();

	auto duration = duration_cast<milliseconds>(system_clock::now() - start);

	cerr << "Sorting" << '\n';
	sort(result.begin(), result.end(), [](const pair<int,double> a, const pair<int,double> b){ return a.first < b.first; });

	cerr << "PageRank in " << structure->getSize() << " node graph in "<< i << " iterations! In " << duration.count() << "ms (error: " << error <<  ") \n";
	//for ( auto it = result.begin(); it != result.end(); it++){
		//printf("%d %.8f\n", it->first, it->second);
	//}
	
	cerr << "PRESS ENTER TO CONTINUE\n";
	cin.get();

	return 0;
}
