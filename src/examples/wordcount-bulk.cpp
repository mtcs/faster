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

void updateOutput(string *& outKeys, int *& outCount, size_t & outSize, unordered_map<string,int> & wordcount){
	outKeys = new string[wordcount.size()];
	outCount = new int[wordcount.size()];
	size_t i = 0;
	for ( auto & it : wordcount ){
		outKeys[i] = it.first;
		outCount[i++] = it.second;
	}
	outSize = wordcount.size();
}

void countWords(string *& outKeys, int *& outCount, size_t & outSize, string * line, size_t inSize){
	unordered_map<string,int> wordcount(inSize);
	for ( size_t i = 0; i < inSize ; i++ ){
		stringstream ss(line[i]);
		string token;
		while (getline(ss, token, ' ')){
			wordcount[token] ++;
		}
	}
	updateOutput(outKeys, outCount, outSize, wordcount);
}

void mergeCounts(string *& outKeys, int *& outCount, size_t & outSize, string * inKey , int * count, size_t inSize){
	unordered_map<string,int> wordcount(inSize);
	for ( size_t i = 0; i < inSize ; i++ ){
		wordcount[inKey[i]] += count[i];
	}
	updateOutput(outKeys, outCount, outSize, wordcount);
}

int main(int argc, char ** argv){

	auto start = system_clock::now();

	// Init Faster Framework
	cout << "Init FastLib" << '\n';
	fastContext fc(argc,argv);
	fc.registerFunction((void*) &countWords, "countWords");
	fc.registerFunction((void*) &mergeCounts, "mergeCounts");
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

	///cerr << "Convert to word count\n";
	//indexedFdd<string,int> * words = data->flatMap(&splitLine);
	//fc.updateInfo();

	cerr << "Count words\n";
	auto wordCount = data->bulkFlatMap(&countWords)->groupByKey()->bulkFlatMap(&mergeCounts);
	fc.updateInfo();

	start2 = system_clock::now();
	wordCount->writeToFile("/tmp/wordcount-","-.txt");
	fc.updateInfo();

	cerr << "  Write Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";
	auto duration = duration_cast<milliseconds>(system_clock::now() - start).count();

	cerr << "Wordcount in " << data->getSize() << " line text In " << duration << "ms \n";

	return 0;
}
