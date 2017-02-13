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

	stringstream ss(input);
	vector<int> edges;
	int key;
	int edge;


	int numTokens = 0;
	for ( size_t i = 0; i < input.size(); ++i ){
		if (input[i] == ' '){
			numTokens++;
		}
	}
	if (numTokens > 2){
		edges.reserve(numTokens-1);
	}// */

	ss >> key;

	while(ss >> edge){
		edges.insert(edges.end(), edge);
	}

	return make_pair(key, std::move(edges));
}// */

pair<int, double> createPR(const int & key, vector<int> & s UNUSED){
	return make_pair(key, 1.0 / numNodes);
}

deque<pair<int, double>> givePageRank(const int & key UNUSED, vector<void *> & sl, vector<void *> & prl){
	auto & s = * (vector<int>*) * sl.begin();
	auto & pr = * (double*) * prl.begin();
	deque<pair<int,double>> msgList;
	double contrib = dumpingFactor * pr / s.size();


	for ( size_t i = 0; i < s.size(); ++i){
		//if (s[i] == 1)
			//cerr << "\033[0;36m" << key << " PR:" << pr << " D:(" << s.size()<< ") S1 : "<< contrib << "\033[0m\n";
		msgList.push_back(make_pair(s[i], contrib));
	}

	return msgList;
}
pair<int, double> combine(const int & key, vector<double *> & prl){
	pair<int,double> r;

	r.first = key;
	r.second = 0;

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

	//if ( key == 1 )
		//cerr << "\n[\033[0;36m1 R:\033[0m";
	for( auto it = contribL.begin(); it != contribL.end(); it++){
		double contrib = *(double*) *it;
		sum += contrib;
		//if ( key == 1 )
			//cerr<< "\033[0;36m"  << contrib << "\033[0m ";
	}
	//pr = (1 - dumpingFactor) + dumpingFactor * sum;
	pr = sum + ( (1.0 - dumpingFactor) / numNodes );
	//if ( key == 1 )
		//cerr << "]\n";

	return fabs(oldPR - pr);
}

pair<int,vector<int>> maxNodeId( const int & ka, vector<int> & a, const int & kb, vector<int> & b){
	vector<int> vout;
	int m = max(ka, kb);

	for(auto it = a.begin(); it != a.end(); it++){
		m = max(m, *it);
	}
	for(auto it = b.begin(); it != b.end(); it++){
		m = max(m, *it);
	}

	return make_pair(m, vout);
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
	fc.registerFunction((void*) &maxNodeId, "maxNodeId");
	fc.registerFunction((void*) &maxError, "maxError");
	fc.registerGlobal(&numNodes);
	fc.startWorkers();
	if (!fc.isDriver())
		return 0;
	cerr << "------------ PageRank -------------\n";

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
	auto structure = data->map<int, vector<int>>(&toAList)->groupByKey()->cache();
	fc.updateInfo();

	//numNodes = structure->getSize();
	numNodes = structure->reduce(&maxNodeId).first + 1;
	fc.updateInfo();
	cerr << numNodes << " node Graph" << '\n';

	cerr << "Init Pagerank\n";
	auto pr = structure->map<int, double>(&createPR)->cache();
	auto iterationData = structure->cogroup(pr)->cache();
	double error = 1000;
	fc.updateInfo();

	cerr << "Process Data\n";
	int i = 0;
	//while( (error >= 1E-4) && (i < 10)){
	while( i < 10 ){
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

		//auto p = pr->collect();
		//sort(p.begin(), p.end());
		//for ( auto i = 0; i < 10 ; i++ ){
			//fprintf(stderr, "\033[0;32m%d:%.8lf\033[0m\n", p[i].first, p[i].second);
		//}
	}
	start2 = system_clock::now();

	pr->writeToFile(std::string("/tmp/pr"), std::string(".txt"));

	cerr << "  Write Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";
	auto duration = duration_cast<milliseconds>(system_clock::now() - start).count();
	cerr << "PageRank in " << structure->getSize() << " node graph in "<< i << " iterations! In " << duration << "ms (error: " << error <<  ") \n";

	//cerr << "PRESS ENTER TO CONTINUE\n";
	//cin.get();
	return 0;
}
