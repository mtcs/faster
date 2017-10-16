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

	vector<size_t> prLocation(numNodes);
	vector<bool> present(numNodes, false);
	vector<double> newPR(numPresNodes, (1 - dumpingFactor) / numNodes);


	if ( (numPresNodes != nPR) || ( nPR != nErrors ) ) {
		cerr << "Internal unknown error 1!!!!";
		cerr << "NPR" << numPresNodes << " npr" << nPR << " nErrors" << nErrors << "\n";
		exit(11);
	}

	//#pragma omp parallel for
	for ( size_t i = 0; i < numPresNodes; ++i){
		if (keys[i] == 2)
			cerr << "FK2 in " << i << " ";
		present[keys[i]] = true;
		prLocation[prKeys[i]] = i;
	}

	//#pragma omp parallel for schedule(dynamic, 200)
	//#pragma omp parallel for
	for ( size_t i = 0; i < numPresNodes; ++i){
		int & key = keys[i];
		double & vertexPR = pr[prLocation[key]];
		double contrib = dumpingFactor * vertexPR / adjList[i].size();
		for ( auto & target : adjList[i] ){
			if ( present[target] ){
				if ( target == 2 )
					cerr << "[\033[0;34m" << key << " " << contrib << ">" << target << "\033[0m] ";
				auto l = prLocation[target];
				//#pragma omp atomic
				newPR[ l ] += contrib;
			}else{
				if ( target == 2 )
					cerr << "L[\033[0;34m" << key << " " << contrib << ">" << target << "\033[0m] ";
				//#pragma omp atomic
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
	givePR.clear();

	// Calculate Page Rank Error
	//#pragma omp parallel for
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
	vector<size_t> prLocation(numNodes);
	vector<bool> vpresent(numNodes, false);

	if ( npr != nErrors ) {
		cerr << "Internal unknown error 2!!!!!!";
		exit(11);
	}

	//#pragma omp parallel for
	for ( size_t i = 0; i < npr; ++i ){
		prLocation[prKeys[i]] = i;
		vpresent[prKeys[i]] = true;
	}
	//#pragma omp parallel for
	for ( size_t i = 0; i < numContribs; ++i){
		auto target = contKeys[i];
		if (! vpresent[target])
			continue;
		size_t targetPrLoc = prLocation[target];
		double cont = contrib[i];
		if ( target == 2 )
			cerr << "[\033[0;34m" << cont << ">" << target << "\033[0m] ";

		//#pragma omp atomic
		newPr[targetPrLoc] += cont;
	}

	//#pragma omp parallel for
	for ( size_t i = 0; i < npr; ++i ){
		// Output PR error
		if ( prKeys[i] == 2 )
			cerr << "[\033[0;35m" << prKeys[i] << " OPR:" << pr[i] << " CPR:" << newPr[i] << " NPR:" << pr[i] + newPr[i] << "\033[0m] ";
		error[i] = fabs(error[i] + newPr[i]);
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
	fastSettings settings;
	if (argc > 3) settings.messageSize = atol(argv[3]);
	fastContext fc(settings, argc, argv);
	//fastContext fc(argc, argv);
	fc.registerFunction((void*) &toAList, "toAList");
	fc.registerFunction((void*) &createPR, "createPR");
	fc.registerFunction((void*) &createErrors, "createErrors");
	fc.registerFunction((void*) &givePageRank, "givePageRank");
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

	cerr << "Import Data" << '\n';
	auto start2 = system_clock::now();
	auto data = new fdd<string>(fc, argv[1]);
	cerr << "  Read Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";
	//cerr << "\033[0;31mPRESS ENTER TO EXIT\033[0m\n"; cin.get();

	cerr << "Convert to Adjacency List\n";
	auto structure = data->map<int, vector<int>>(&toAList);
	fc.updateInfo();
	structure->groupByKey()->cache();
	//cerr << "\033[0;31mPRESS ENTER TO EXIT\033[0m\n"; cin.get();
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
		auto contribs = iterationData->bulkFlatMap(&givePageRank)->cache();
		fc.updateInfo();

		cerr << contribs->getSize() << " (" << contribs->getSize() << ") messages. (" << contribs->getSize()*(sizeof(double) + sizeof(int))/1024/1024 << "MB)\n";

		pr->cogroup(contribs, errors)->bulkUpdate(&getNewPR);
		error = errors->reduce(&maxError).second;
		fc.updateInfo();
		cerr << "  Error " << error << " time:" << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";

		int j = 0;
		auto p = pr->collect();
		sort(p.begin(), p.end());
		for ( auto & it : p ){
			if (j++ >10) break;
			fprintf(stderr, "\033[0;32m%d:%.8lf\033[0m  ", it.first, it.second);
		} // */
		//cerr << "\033[0;31mPRESS ENTER TO EXIT\033[0m\n"; cin.get();
		//return(false);
	}
	start2 = system_clock::now();

	pr->writeToFile(std::string("/tmp/pr"), std::string(".txt"));

	auto duration = duration_cast<milliseconds>(system_clock::now() - start).count();
	cerr << "  Write Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";
	cerr << "PageRank in " << structure->getSize() << " node graph in "<< i << " iterations! In " << duration << "ms (error: " << error <<  ") \n";

	//cerr << "\033[0;31mPRESS ENTER TO EXIT\033[0m\n"; cin.get();

	return 0;
}
