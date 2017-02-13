#include <iostream>
#include <sstream>

#include <deque>

#include "libfaster.h"

#define NUMITEMS (100*1000)

using namespace std;
using namespace faster;

using std::chrono::system_clock;
using std::chrono::duration_cast;
using std::chrono::milliseconds;

deque<pair<string,int>> splitLine(string & line){
	deque<pair<string,int>> result;
	stringstream ss(line);
	string token;

	while (getline(ss, token, ' ')){
		result.insert(result.end(), make_pair(token, 1));
	}

	return result;
}

pair<string, int> countWords(const string & key, vector<int *> & countV){
	int count = 0;
	for ( auto v : countV){
		count += *v;
	}
	return make_pair(key, count);
}

int main(int argc, char ** argv){

	auto start = system_clock::now();

	// Init Faster Framework
	cout << "Init FastLib" << '\n';
	fastContext fc(argc,argv);
	fc.registerFunction((void*) &splitLine, "splitLine");
	fc.registerFunction((void*) &countWords, "countWords");
	fc.startWorkers();
	if (!fc.isDriver())
		return 0;
	cerr << "------------ WordCount -------------\n";
	fc.printHeader();

	cerr << "Import Data\n";
	auto start2 = system_clock::now();
	auto data = new fdd<string> (fc, argv[1]);
	fc.updateInfo();
	cerr << "  Read Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";

	cerr << "Convert to word count\n";
	indexedFdd<string,int> * localWordCount = data->flatMap(&splitLine);
	fc.updateInfo();

	auto wordCount = localWordCount->mapByKey(&countWords)->groupByKey()->mapByKey(&countWords);
	fc.updateInfo();

	start2 = system_clock::now();
	wordCount->writeToFile("/tmp/wordcount-","-.txt");
	fc.updateInfo();

	cerr << "  Write Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";
	auto duration = duration_cast<milliseconds>(system_clock::now() - start).count();

	cerr << "Wordcount in " << data->getSize() << " line text In " << duration << "ms \n";

	return 0;
}
