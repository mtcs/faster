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

pair<int, double> createErrors(const int & key, vector<int> & s UNUSED){
	return make_pair(key, 0);
}

deque<pair<int, double>> givePageRank(int * keys, void * adjListP, size_t numPresNodes, int * prKeys, void * prP, size_t nPR UNUSED, int * eKeys UNUSED, void * errorsP, size_t nErrors UNUSED){
	auto adjList = (vector<int>*) adjListP;
	auto pr = (double*) prP;
	auto errors = (double*) errorsP;
	deque<pair<int,double>> msgList;

	vector<size_t> prLocation(numNodes+1);
	vector<bool> present(numNodes+1, false);
	vector<double> newPR(numPresNodes);


	if ( (numPresNodes != nPR) || ( nPR != nErrors ) ) { 
		cerr << "Internal unknown error!!!!";
		exit(11);
	}

	//#pragma omp parallel
	{
	//	#pragma omp for
		for ( size_t i = 0; i < numPresNodes; ++i){
			present[keys[i]] = true;
			prLocation[prKeys[i]] = i;
			newPR[i] = (1 - dumpingFactor);
		}

		//#pragma omp for
		for ( size_t i = 0; i < numPresNodes; ++i){
			double contrib = dumpingFactor * pr[prLocation[keys[i]]] / adjList[i].size();
			for ( size_t j = 0; j < adjList[i].size(); ++j){
				auto target = adjList[i][j];

				if ( present[target] ){
					//#pragma omp atomic
					newPR[ prLocation[target] ] += contrib;
				}else{
					auto p = make_pair(adjList[i][j], contrib);

					//#pragma omp critical
					msgList.push_back(p);
				}
			}
		}

		//#pragma omp for
		for ( size_t i = 0; i < numPresNodes; ++i){
			errors[i] = newPR[i] - pr[i];
			pr[i] = newPR[i];
		}
	}

	return msgList;
}

//pair<int, double> combine(const int & key, deque<double *> * prl){
void combine(int *& outKeys, double *& outPr, size_t & nOut, int * contKey, double * prCont, size_t numCont){
	vector<double> outMsgVec(numNodes, 0);
	size_t numOutMsgs = 0;

	//#pragma omp for
	for ( size_t i = 0; i < numCont; i++){
		double contrib = prCont[i];
		//if ( outMsgVec[contKey[i]] == 0 ) numOutMsgs++;
		//#pragma omp atomic
		outMsgVec[contKey[i]] += contrib;
	}

	//#pragma omp for
	for ( size_t i = 0; i < numNodes; i++){
		if ( outMsgVec[i] > 0 ){
			//#pragma omp atomic
			numOutMsgs ++;
		}
	}
	
	outKeys = new int[numOutMsgs];
	outPr = new double[numOutMsgs];
	nOut = numOutMsgs;

	size_t pos = 0;
	//#pragma omp parallel for
	for ( size_t i = 0; i < numNodes; i++){
		if ( outMsgVec[i] > 0 ){
			size_t myPos;
			
			//#pragma omp atomic capture
			myPos = pos++;

			outKeys[myPos] = i;
			outPr[myPos] = outMsgVec[i];
		}
	}

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
	
	//#pragma omp parallel
	{
		//#pragma omp for
		for ( size_t i = 0; i < npr; ++i ){
			prLocation[prKeys[i]] = i;
			//eLocation[eKeys[i]] = i;
		}
		//#pragma omp for
		for ( size_t i = 0; i < numContribs; ++i){
			auto target = contKeys[i];
			size_t targetPrLoc = prLocation[target];

			//#pragma omp atomic
			newPr[targetPrLoc] += contrib[i];
		}

		//#pragma omp for
		for ( size_t i = 0; i < npr; ++i ){
			// Output PR error
			error[i] = abs(error[i] + newPr[i]);
			pr[i] += newPr[i];

		}
	}
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
	fc.registerFunction((void*) &combine, "combine");
	fc.registerFunction((void*) &getNewPR, "getNewPR");
	fc.registerFunction((void*) &maxError, "maxError");
	fc.registerGlobal(&numNodes);
	fc.startWorkers();

	fc.printHeader();

	if ( (argc > 2) && (argv[2][0] == '1') ){
		cerr << "Calibrate Performance\n";
		fc.calibrate();
		fc.updateInfo();
	}

	cerr << "Import Data" << '\n';
	auto data = new fdd<string>(fc, argv[1]);
	auto structure = data->map<int, vector<int>>(&toAList)->cache();
	numNodes = structure->getSize();
	fc.updateInfo();

	cerr << "Init Pagerank" << '\n';
	auto pr = structure->map<int, double>(&createPR)->cache();
	auto errors = structure->map<int, double>(&createErrors)->cache();
	auto iterationData = structure->cogroup(pr, errors);
	fc.updateInfo();
	double error = 1000;

	cerr << "Process Data" << '\n';
	int i = 0;
	while( error >= 1){
		cerr << "\033[1;32mIteration " << i++ << "\033[0m\n";
		auto start2 = system_clock::now();
		auto contribs = iterationData->bulkFlatMap(&givePageRank);
		fc.updateInfo();

		auto combContribs = contribs->bulkFlatMap(&combine);
		fc.updateInfo();
		cerr << contribs->getSize() << " (" << combContribs->getSize() << ") messages. \n";

		pr->cogroup(combContribs, errors)->bulkUpdate(&getNewPR);
		error = errors->reduce(&maxError).second;
		fc.updateInfo();
		cerr << "  Error " << error << " time:" << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";

	}
	auto result = pr->collect();

	auto duration = duration_cast<milliseconds>(system_clock::now() - start);

	cerr << "Sorting" << '\n';
	sort(result.begin(), result.end(), [](const pair<int,double> a, const pair<int,double> b){ return a.first < b.first; });

	cerr << "PageRank in " << structure->getSize() << " node graph in "<< i << " iterations! In " << duration.count() << "ms (error: " << error <<  ") \n";
	for ( auto it = result.begin(); it != result.end(); it++){
		printf("%d %.8f\n", it->first, it->second);
	}

	return 0;
}
