#include <iostream>
#include <algorithm>
#include "libfaster.h"

using namespace std;
using namespace faster;

using std::chrono::system_clock;
using std::chrono::duration_cast;
using std::chrono::milliseconds;

size_t matSize;
size_t batchSize = 16;
//vector<float> globalCentroidsX;
//vector<float> globalCentroidsY;
//float * globalCentroidsX = NULL;
//float * globalCentroidsY = NULL;
vector<vector<float>> columns;

typedef vector<float> line_t;

pair<int,vector<float>> toVector(string & input){

	stringstream ss(input);
	vector<float> position;
	position.reserve(2);
	int key = 0;
	float val;

	ss >> key;

	while(ss >> val){
		position.insert(position.end(), val);
	}

	return make_pair(key, move(position));
}// */

pair<int, vector<float>> multLines(const int & key, line_t & line){
	pair<int, vector<float>> result;
	result.first = key;
	result.second.resize(batchSize);
	for ( size_t k = 0; k < batchSize ; k++ ){
		result.second[k] = 0;
	}

	for ( size_t i = 0; i < line.size() ; i+=batchSize ){
		for ( size_t k = 0; k < batchSize ; k++ ){
			for ( size_t l = i; l < i+batchSize ; l++ ){
				result.second[k] += line[l]*columns[k][l];
			}
		}
	}
	return result;
}

void copyColumnsToGlobal(vector<pair<int,line_t>> & m2, size_t j){
	for ( size_t k = 0; (k < batchSize) && ((j+k) < matSize)  ; k++ ){
		copy(m2[j+k].second.begin(), m2[j+k].second.end(), columns[k].begin());
	}
}

vector<line_t> matMult(fastContext & fc UNUSED, indexedFdd<int,line_t> * m1, vector<pair<int,line_t>> & m2){
    	// Alloc result matrix
	vector<line_t> resultMat(matSize);
	for ( auto & l : resultMat ){
		l.resize(matSize);
	}

	for ( size_t j = 0; j < matSize ; j+=batchSize ){
		// Copy a batch of columns to global var
		copyColumnsToGlobal(m2, j);

		// Calculate result
		auto newLines = m1->map(&multLines);

		// Get result
		vector<pair<int,vector<float>>> localNewLines = newLines->collect();

		// Save in result matrix
		for ( auto & v : localNewLines ) {
			for ( size_t k = 0; (k < batchSize) && ((j+k) < matSize) ; k++ ){
				resultMat[v.first][m2[j+k].first] = v.second[k];
			}
		}
		fc.updateInfo();
		newLines->discard();
	}

	return resultMat;
}

int main(int argc, char ** argv){
	int numItems;
	// Init Faster Framework
	auto start = system_clock::now();

	fastContext fc(argc, argv);

	if (argc > 3) matSize = atoi(argv[3]);
	if (argc > 4) batchSize = atoi(argv[4]);
	columns.resize(batchSize);
	for ( auto & v : columns )
		v.resize(matSize);

	fc.registerFunction((void*) &toVector, "toVectors");
	fc.registerFunction((void*) &multLines, "multLines");

	fc.registerGlobal(&columns);
	fc.registerGlobal(&batchSize);
	fc.startWorkers();
	if (!fc.isDriver())
		return 0;

	cerr << "------------ Matrix Multiplication -------------\n";

	fc.printHeader();
	cerr << "Init Time: " << duration_cast<milliseconds>(system_clock::now() - start).count() << "ms\n";

	cerr << "Import Data\n";
	auto start2 = system_clock::now();
	auto strdata = new fdd<string>(fc, argv[1]);
	auto strdata2 = new fdd<string>(fc, argv[2]);
	cerr << "  Read Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";

	cerr << "Convert to Vector\n";
	auto m1 = strdata->map(&toVector)->cache();
	auto m2 = strdata2->map(&toVector)->collect();
	numItems = m1->getSize();
	fc.updateInfo();

	cerr << "\033[1;33mInit MMult (" << numItems << " rank Matrix)\033[0m\n";

	auto result = matMult(fc, m1, m2);

	start2 = system_clock::now();

	//clusterAssign->writeToFile(std::string("/tmp/clusterAssign"), std::string(".txt"));

	auto duration = duration_cast<milliseconds>(system_clock::now() - start).count();
	cerr << "  Write Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";
	cerr << "MMult of 2 " << m1->getSize()
		<< " rank matrices "
		<< " In " << duration
		<< "ms \n";
}
