#include <iostream>
#include <fstream>
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
vector< pair<int,vector<float>> > m2;

typedef vector<float> line_t;

pair<int,vector<float>> toVector(string & input){

	stringstream ss(input);
	vector<float> position;
	position.reserve(matSize);
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
	result.second.resize(m2.size());
	for ( size_t k = 0; k < batchSize ; k++ ){
		result.second[k] = 0;
	}

	for ( size_t i = 0; i < line.size() ; i+=batchSize ){
		for ( size_t j = 0; j < m2.size() ; j+=batchSize ){
			for ( size_t l = i; l < i+batchSize ; l++ ){
				for ( size_t m = j; m < j+batchSize ; m++ ){
					result.second[m2[m].first] += line[l] * m2[m].second[l];
				}
			}
		}
	}
	return result;
}

indexedFdd<int, line_t> * matMult(fastContext & fc UNUSED, indexedFdd<int,line_t> * m1){
	// Calculate result

	auto resultMat = m1->map(&multLines);

	fc.updateInfo();

	return resultMat;
}
 void readMatrix(string fileName, vector<pair<int,line_t>> & mat){
	ifstream input(fileName, std::ifstream::in);
	int id;
	size_t i = 0;
	mat.resize(matSize);

	while (input.good()){
		input >> id;
		mat[i].first = id;
		mat[i].second.resize(matSize);
		for ( size_t j = 0; j < matSize ; j++ ){
			input >> mat[i].second[j];
		}
		i++;
	}
}

int main(int argc, char ** argv){
	int numItems;
	// Init Faster Framework
	auto start = system_clock::now();

	fastContext fc(argc, argv);

	if (argc > 3) matSize = atoi(argv[3]);
	if (argc > 4) batchSize = atoi(argv[4]);
	cerr << "Read M2 " << argv[2] << "\n";
	readMatrix(argv[2], m2);
	cerr << "\n";

	fc.registerFunction((void*) &toVector, "toVectors");
	fc.registerFunction((void*) &multLines, "multLines");

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
	//auto m1 = new indexedFdd<int,vector<float>>(fc, argv[1]);
	cerr << "  Read Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";

	cerr << "Convert to Vector\n";
	auto m1 = strdata->map(&toVector)->cache();
	numItems = m1->getSize();
	fc.updateInfo();

	cerr << "\033[1;33mInit MMult (" << numItems << " rank Matrix) " << batchSize << " Batch \033[0m\n";

	auto result = matMult(fc, m1);

	start2 = system_clock::now();

	result->writeToFile(std::string("/tmp/m3"), std::string(".txt"));

	auto duration = duration_cast<milliseconds>(system_clock::now() - start).count();
	cerr << "  Write Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";
	cerr << "MMult of 2 " << m1->getSize()
		<< " rank matrices "
		<< " In " << duration
		<< "ms \n";
}
