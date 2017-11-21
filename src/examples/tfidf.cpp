#include <iostream>
#include <sstream>

#include <deque>

#include "libfaster.h"

using namespace std;
using namespace faster;

using std::chrono::system_clock;
using std::chrono::duration_cast;
using std::chrono::milliseconds;

int numDocuments = 0;
unordered_map<string, double> idf;

deque<pair<string,int>> docThermCount(string & line){
	unordered_map<string,int> thermCount;
	stringstream ss(line);
	string token;

	while (getline(ss, token, ' ')){
		auto it = thermCount.find(token);
		if (it == thermCount.end())
			thermCount[token] = 1;
	}
	deque<pair<string,int>> result(thermCount.begin(), thermCount.end());

	return result;
}

pair<string, int> sumThermCount(const string & key, vector<int *> & countV){
	int count = 0;
	for ( auto v : countV){
		count += *v;
	}
	return make_pair(key, count);
}

pair<string, double> getIDF(const string & key, int & count){
	return make_pair(key, double(numDocuments)/count);
}

string getTfidf(string & line){
	unordered_map<string,int> thermCount;
	stringstream ss(line);
	string token;

	while (getline(ss, token, ' ')){
		auto it = thermCount.find(token);
		if (it == thermCount.end())
			thermCount[token] = 1;
		else
			it->second += 1;
	}

	stringstream resultss;
	for ( auto & it : thermCount ){
		resultss << it.first << ":";
		resultss << idf[it.first] * it.second / thermCount.size() << " ";
	}
	return resultss.str();
}

int main(int argc, char ** argv){

	auto start = system_clock::now();

	// Init Faster Framework
	cout << "Init FastLib" << '\n';
	fastContext fc(argc,argv);
	fc.registerFunction((void*) &docThermCount, "docThermCount");
	fc.registerFunction((void*) &sumThermCount, "sumThermCount");
	fc.registerFunction((void*) &getIDF, "getIDF");
	fc.registerFunction((void*) &getTfidf, "getTfidf");
	fc.registerGlobal(&numDocuments);
	fc.registerGlobal(&idf);
	fc.startWorkers();
	if (!fc.isDriver())
		return 0;
	cerr << "------------ WordCount -------------\n";
	fc.printHeader();

	cerr << "Import Data " << argv[1] << "\n";
	auto start2 = system_clock::now();
	auto data = new fdd<string> (fc, argv[1]);
	data->cache();
	numDocuments = data->getSize();
	fc.updateInfo();
	cerr << "  Read Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";

	cerr << "Convert to word count\n";
	indexedFdd<string,int> * parcialThermCount = data->flatMap(&docThermCount);
	fc.updateInfo();
	cerr << parcialThermCount->getSize() << " Therms\n";

	cerr << "Count Therms\n";
	auto thermCountByDoc = parcialThermCount->mapByKey(&sumThermCount);
	fc.updateInfo();
	cerr << thermCountByDoc->getSize() << " Therms\n";
	thermCountByDoc = thermCountByDoc->groupByKey()->mapByKey(&sumThermCount);
	fc.updateInfo();
	cerr << thermCountByDoc->getSize() << " Therms\n";

	cerr << "Get IDF\n";
	auto idfV = thermCountByDoc->map(getIDF)->collect();
	fc.updateInfo();
	//cin.get();
	cerr << "Update Global IDF\n";
	//idf.assign(idf.begin(), idfV.begin(), idfV.end());
	for ( auto & it : idfV ){
		idf.insert(it);
	}
	cerr << "Get TFIDF\n";
	auto tfidf = data->map(getTfidf);
	fc.updateInfo();

	start2 = system_clock::now();
	tfidf->writeToFile("/tmp/wordcount-",".txt");
	fc.updateInfo();

	cerr << "  Write Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";
	auto duration = duration_cast<milliseconds>(system_clock::now() - start).count();

	cerr << "Wordcount in " << data->getSize() << " line text In " << duration << "ms \n";
	//getc(stdin);

	return 0;
}
