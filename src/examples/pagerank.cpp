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

size_t numNodes = 0;

pair<int,vector<int>> toAList(string & input){
	long int lastPos = -1;
	deque<pair<size_t,size_t>> pos;


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

pair<int, double> createPR(const int & key, vector<int> & s UNUSED){
	return make_pair(key, 1);
}

deque<pair<int, double>> givePageRank(const int & key UNUSED, vector<void *> & sl, vector<void *> & prl){
	auto & s = * (vector<int>*) * sl.begin();
	auto & pr = * (double*) * prl.begin();
	deque<pair<int,double>> msgList;
	double contrib = dumpingFactor * pr / s.size();


	for ( size_t i = 0; i < s.size(); ++i){
		msgList.push_back(make_pair(s[i], contrib));
	}
	
	return msgList;
}
pair<int, double> combine(const int & key, vector<double *> & prl){
	pair<int,double> r;

	r.first = key;
		
	//cerr << key << ":" << prl.size() << " ";
	for ( auto it = prl.begin(); it != prl.end(); it++){
		r.second += **it;
	}

	return r;
}

double getNewPR(const int & key UNUSED, vector<void *> & prL, vector<void *> & contribL){
	//cerr << key << " ";
	double & pr = * (double*) * prL.begin();
	double oldPR = pr;
	double sum = 0;


	for( auto it = contribL.begin(); it != contribL.end(); it++){
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
	fc.registerFunction((void*) &toAList, "toAList");
	fc.registerFunction((void*) &createPR, "createPR");
	fc.registerFunction((void*) &givePageRank, "givePageRank");
	fc.registerFunction((void*) &combine, "combine");
	fc.registerFunction((void*) &getNewPR, "getNewPR");
	fc.registerFunction((void*) &maxError, "maxError");
	fc.registerGlobal(&numNodes);
	fc.startWorkers();

	fc.printHeader();
	cerr << "  Init Time: " << duration_cast<milliseconds>(system_clock::now() - start).count() << "ms\n";

	if ( (argc > 2) && (argv[2][0] == '1') ){
		cerr << "Calibrate Performance\n";
		fc.calibrate();
		fc.updateInfo();
	}

	cerr << "Import Data\n";
	auto start2 = system_clock::now();
	auto data = new fdd<string>(fc, argv[1]);
	cerr << "  Read Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";

	cerr << "Convert to Adjacency List\n";
	auto structure = data->map<int, vector<int>>(&toAList)->cache();
	fc.updateInfo();

	numNodes = structure->getSize();

	cerr << "Init Pagerank\n";
	auto pr = structure->map<int, double>(&createPR)->cache();
	auto iterationData = structure->cogroup(pr);
	double error = 1000;
	fc.updateInfo();

	cerr << "Process Data\n";
	int i = 0;
	while( error >= 1){
		cerr << "\033[1;32mIteration " << i++ << "\033[0m\n" ;
		start2 = system_clock::now();
		auto contribs = iterationData->flatMapByKey(&givePageRank);
		fc.updateInfo();

		auto combContribs = contribs->mapByKey(&combine);
		fc.updateInfo();

		cerr << "  " << contribs->getSize() << " (" << combContribs->getSize() << ") messages.\n";

		error = pr->cogroup(combContribs)->mapByKey(&getNewPR)->reduce(&maxError);
		fc.updateInfo();
		cerr << "  Error " << error << " time:" << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";
	}
	start2 = system_clock::now();
	auto result = pr->collect();

	cerr << "  Collect Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";
	start2 = system_clock::now();

	for ( auto it = result.begin(); it != result.end(); it++){
		printf("%d %.8f\n", it->first, it->second);
	}
	auto duration = duration_cast<milliseconds>(system_clock::now() - start).count();
	cerr << "  Write Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";
	cerr << "PageRank in " << structure->getSize() << " node graph in "<< i << " iterations! In " << duration << "ms (error: " << error <<  ") \n";
	
	//cerr << "PRESS ENTER TO CONTINUE\n";
	//cin.get();

	return 0;
}
