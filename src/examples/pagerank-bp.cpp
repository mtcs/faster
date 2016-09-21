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
size_t numProcs = 0;


int currNumNodes = 0;
vector<int> budget;
vector<int> partAssignment(1000000, -1);

int greedyPartition(int & k, vector<int> & d){

	static bool started = false;
	vector<int> neighbPartCount(numProcs, 0);


	if (! started){
		budget.resize(numProcs,0);
		started = true;
	}

	currNumNodes++;

	if ( size_t(k) >= partAssignment.size()){
		partAssignment.resize(max(size_t(k+1), size_t(partAssignment.size()*1.5)), -1);
	}

	for ( size_t i = 0; i < d.size(); i++){
		if ( size_t(d[i]) >= partAssignment.size())
			partAssignment.resize(max(size_t(d[i]+1), size_t(partAssignment.size()*1.5)), -1);

		int p = partAssignment[d[i]];

		if ( p >= 0 )
			neighbPartCount[p]++;
	}
	//cerr << "Scores: ";

	int myPart = partAssignment[k];
	float myScore = -1;
	for ( size_t i = 1; i < neighbPartCount.size(); i++){
		//cerr << neighbPartCount[i] << ":";
		float score = neighbPartCount[i] * (1 - ( (float) budget[i] / currNumNodes ) );
		//cerr << score  << " ";

		if ( myScore < score ){
			myScore = score;
			myPart = i;
		}else{
			if ( ( myScore == score ) && ( budget[i] < budget[myPart] ) ){
				myPart = i;
			}
		}
	}
	//cerr << "\n"; cerr.flush();
	//cerr << myPart << " ";

	budget[myPart] ++;
	partAssignment[k] = myPart;

	return myPart;
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


	if ( (numPresNodes != nPR) || ( numPresNodes != nErrors ) ) {
		cerr << "Internal unknown error!!!!";
		exit(11);
	}

	#pragma omp parallel for
	for ( size_t i = 0; i < numPresNodes; ++i){
		present[keys[i]] = true;
		prLocation[prKeys[i]] = i;
	}

	#pragma omp parallel for schedule(dynamic, 500)
	for ( size_t i = 0; i < numPresNodes; ++i){
		double contrib = dumpingFactor * pr[prLocation[keys[i]]] / adjList[i].size();
		for ( size_t j = 0; j < adjList[i].size(); ++j){
			auto target = adjList[i][j];

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

	//#pragma omp parallel for
	for ( size_t i = 0; i < npr; ++i ){
		prLocation[prKeys[i]] = i;
		//eLocation[eKeys[i]] = i;
	}
	//#pragma omp parallel for
	for ( size_t i = 0; i < numContribs; ++i){
		auto target = contKeys[i];
		size_t targetPrLoc = prLocation[target];
		double cont = contrib[i];

		//#pragma omp atomic
		newPr[targetPrLoc] += cont;
	}

	//#pragma omp parallel for
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
	fc.registerFunction((void*) &greedyPartition, "greedyPartition");
	fc.registerFunction((void*) &createPR, "createPR");
	fc.registerFunction((void*) &createErrors, "createErrors");
	fc.registerFunction((void*) &givePageRank, "givePageRank");
	fc.registerFunction((void*) &getNewPR, "getNewPR");
	fc.registerFunction((void*) &maxNodeId, "maxNodeId");
	fc.registerFunction((void*) &maxError, "maxError");
	fc.registerGlobal(&numNodes);
	fc.registerGlobal(&numProcs);
	fc.startWorkers();
	if (!fc.isDriver())
		return 0;
	// !!!!!!!! THIS EXAMPLE IS NOT SUPOSED TO RUN !!!!!!!!!!!
	// !!!!!!!! IT HAS NOT BEEN IMPLEMENTED YET    !!!!!!!!!!!
	return 0;

	fc.printHeader();
	cerr << "  Init Time: " << duration_cast<milliseconds>(system_clock::now() - start).count() << "ms\n";

	if ( (argc > 2) && (argv[2][0] == '1') ){
		cerr << "Calibrate Performance\n";
		fc.calibrate();
		fc.updateInfo();
	}
	numProcs = fc.numProcs();

	cerr << "Import Data" << '\n';
	auto start2 = system_clock::now();
	auto structure = fc.onlineFullPartRead(string(argv[1]), &greedyPartition)->cache();
	partAssignment.clear();
	cerr << "  Read Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";

	//cerr << "Convert to Adjacency List\n";
	//auto structure = data->map<int, vector<int>>(&toAList)->cache();
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
	}
	start2 = system_clock::now();

	pr->writeToFile(std::string("/tmp/pr"), std::string(".txt"));

	auto duration = duration_cast<milliseconds>(system_clock::now() - start).count();
	cerr << "  Write Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";
	cerr << "PageRank in " << numNodes << " node graph in "<< i << " iterations! In " << duration << "ms (error: " << error <<  ") \n";

	//cerr << "\033[0;31mPRESS ENTER TO EXIT\033[0m\n";
	//cin.get();

	return 0;
}
