#include <iostream>
#include <algorithm>
#include "libfaster.h"

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

pair<int, double> createErrors(const int & key, vector<int> & s UNUSED){
	return make_pair(key, 1);
}

deque<pair<int, double>> givePageRank(int * keys, void * adjListP, size_t numPresNodes, int * prKeys, void * prP, size_t nPR UNUSED, int * eKeys UNUSED, void * errorsP, size_t nErrors UNUSED){
	auto adjList = (vector<int>*) adjListP;
	auto pr = (double*) prP;
	auto errors = (double*) errorsP;
	deque<pair<int,double>> msgList;
	vector<double> givePR(numNodes,0);

	vector<size_t> prLocation(numNodes+1);
	vector<bool> present(numNodes+1, false);
	vector<double> newPR(numPresNodes, (1 - dumpingFactor) / numNodes);


	if ( (numPresNodes != nPR) || ( nPR != nErrors ) ) { 
		cerr << "Internal unknown error!!!!";
		exit(11);
	}

	#pragma omp parallel for
	for ( size_t i = 0; i < numPresNodes; ++i){
		present[keys[i]] = true;
		prLocation[prKeys[i]] = i;
	}

	#pragma omp parallel for schedule(dynamic, 200)
	//#pragma omp parallel for 
	for ( size_t i = 0; i < numPresNodes; ++i){
		double contrib = dumpingFactor * pr[prLocation[keys[i]]] / adjList[i].size();
		for ( size_t j = 0; j < adjList[i].size(); ++j){
			auto target = adjList[i][j];
			/*if (target == 1){
				cerr << "\033[0;36m" << keys[i]  << " PR:" << pr[prLocation[keys[i]]] << " D:(" << adjList[i].size() << ") S1 : "<< contrib ;
				if ( present[target] ){
					cerr << "[LOCAL]";
				}
				cerr << "\033[0m\n";
			}// */

			if ( present[target] ){
				auto l = prLocation[target];
				#pragma omp atomic
				newPR[ l ] += contrib;
			}else{
				#pragma omp atomic
				givePR[target] += contrib;
			}
		}
	}
	// Generate messages
	for ( size_t i = 0; i < numNodes; ++i){
		if ( givePR[i] != 0 ){
			auto p = make_pair(i, givePR[i]);

			msgList.push_back(p);
		}
	}

	// Calculate Page Rank Error
	#pragma omp parallel for
	for ( size_t i = 0; i < numPresNodes; ++i){
		errors[i] = newPR[i] - pr[i];
		pr[i] = newPR[i];
		//if ( prKeys[i] == 1 )
			//cerr << "\033[0;36m" << " TPR:" << pr[i] << " D:(" << adjList[i].size() << ") S1 : " << newPR[i] << "\033[0m\n";
	}

	return msgList;
}

void getNewPR(int * prKeys, void * prVP, size_t npr, int * contKeys, void * contribVP, size_t numContribs, int * eKeys UNUSED, void * errorVP, size_t nErrors UNUSED){
	double * pr =  (double*) prVP;
	double * contrib = (double*) contribVP;
	double * error = (double*) errorVP;

	vector<double> newPr(npr, 0);
	vector<size_t> prLocation(numNodes+1);

	if ( npr != nErrors ) {
		cerr << "Internal unknown error!!!!!!";
		exit(11);
	}
	
	#pragma omp parallel for
	for ( size_t i = 0; i < npr; ++i ){
		prLocation[prKeys[i]] = i;
		//eLocation[eKeys[i]] = i;
	}
	#pragma omp parallel for
	for ( size_t i = 0; i < numContribs; ++i){
		auto target = contKeys[i];
		size_t targetPrLoc = prLocation[target];
		double cont = contrib[i];

		//#pragma omp atomic
		newPr[targetPrLoc] += cont;
		//if ( target == 1 )
			//cerr << "[\033[0;36m1 R:" << cont << "\033[0m] \n";
	}

	#pragma omp parallel for
	for ( size_t i = 0; i < npr; ++i ){
		// Output PR error
		error[i] = abs(error[i] + newPr[i]);
		pr[i] += newPr[i];

	}
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

pair<int,double> maxError(const int & ka, double & a, const int & kb, double & b){
	if (a > b)
		return make_pair(ka,a);
	else
		return make_pair(kb,b);
}



int main(int argc, char ** argv){
	// Init Faster Framework

	auto start = system_clock::now();

	fastContext fc(argc, argv);
	fc.registerFunction((void*) &toAList, "toAList");
	fc.registerFunction((void*) &createPR, "createPR");
	fc.registerFunction((void*) &createErrors, "createErrors");
	fc.registerFunction((void*) &givePageRank, "givePageRank");
	fc.registerFunction((void*) &getNewPR, "getNewPR");
	fc.registerFunction((void*) &maxNodeId, "maxNodeId");
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

	cerr << "Import Data" << '\n';
	auto start2 = system_clock::now();
	auto data = new fdd<string>(fc, argv[1]);
	cerr << "  Read Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";

	cerr << "Convert to Adjacency List\n";
	auto structure = data->map<int, vector<int>>(&toAList)->groupByKey()->cache();
	fc.updateInfo();

	numNodes = structure->reduce(&maxNodeId).first + 1;
	fc.updateInfo();
	cerr << numNodes << " node Graph" << '\n';

	cerr << "Init Pagerank" << '\n';
	auto pr = structure->map<int, double>(&createPR)->cache();
	auto errors = structure->map<int, double>(&createErrors)->cache();
	auto iterationData = structure->cogroup(pr, errors)->cache();
	fc.updateInfo();
	double error = 1;

	cerr << "Process Data" << '\n';
	int i = 0;
	while( i < 10 ){
		cerr << "\033[1;32mIteration " << i++ << "\033[0m\n";
		auto start2 = system_clock::now();
		auto contribs = iterationData->bulkFlatMap(&givePageRank);
		fc.updateInfo();
		cerr << contribs->getSize() << " (" << contribs->getSize() << ") messages. \n";

		pr->cogroup(contribs, errors)->bulkUpdate(&getNewPR);
		error = errors->reduce(&maxError).second;
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

	auto duration = duration_cast<milliseconds>(system_clock::now() - start).count();
	cerr << "  Write Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";
	cerr << "PageRank in " << structure->getSize() << " node graph in "<< i << " iterations! In " << duration << "ms (error: " << error <<  ") \n";

	//cerr << "\033[0;31mPRESS ENTER TO EXIT\033[0m\n";
	//cin.get();

	return 0;
}
