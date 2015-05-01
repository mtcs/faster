#include <iostream>
#include "libfaster.h"

using namespace std;
using namespace faster;

using std::chrono::system_clock;
using std::chrono::duration_cast;
using std::chrono::milliseconds;

const int numCentroids = 10;
vector< vector<int> > globalCentroids;

pair<int,vector<int>> toVector(string & input){
	
	stringstream ss(input);
	vector<int> position(2);
	//int key;
	int val;

	//ss >> key;

	while(ss >> val){
		position.insert(position.end(), val);
	}

	return make_pair(0, move(position));
}// */

int vecDistance(vector<int> &a, vector<int> &b){
	int dx = a[0] - b[0];
	int dy = a[1] - b[1];
	return sqrt(dx*dx + dy*dy);
}

void updateCentroidAssignment(int & key, std::vector<int> & input){
	int closer = 0;
	float minDistance = vecDistance(input, globalCentroids[0]);
	for ( int i = 1; i < numCentroids; i++ ){
		float dist = vecDistance(input, globalCentroids[i]);
		if ( minDistance > dist ){
			closer = i;
			minDistance = dist;
		}
	}
	key = closer;
}

void updateCentroids(const int & key UNUSED, std::vector<void*> & items, std::vector<void*> & centroidPosP, std::vector<void*> & errorP){
	vector<int> * centroidPos  = (vector<int> *) centroidPosP[0];
	int * error = (int *) errorP[0];

	int sumX = 0;
	int sumY = 0;
	for (size_t i = 0; i < items.size(); i++){
		vector<int> * item = (vector<int> *) items[i];
		sumX += (*item)[0];
		sumY += (*item)[1];
	}
	vector<int> newPos (2);
	newPos[0] = sumX / items.size();
	newPos[0] = sumY / items.size();
	*error = vecDistance( *centroidPos, newPos);
	(*centroidPos) = newPos;
}

pair<int,float> maxError(const int & keyA UNUSED, float & a, const int & keyB UNUSED, float & b){
	return make_pair(0, max(a,b));
}

int main(int argc, char ** argv){
	int numItems;
	globalCentroids.resize(numCentroids);

	// Init Faster Framework
	cerr << "------------ K-MEANS -------------";

	auto start = system_clock::now();

	fastContext fc(argc, argv);
	fc.registerFunction((void*) &toVector, "toVectors");
	fc.registerFunction((void*) &updateCentroidAssignment, "updateCentroidAssignment");
	fc.registerFunction((void*) &updateCentroids, "updateCentroids");
	fc.registerFunction((void*) &maxError, "maxError");

	fc.registerGlobal(&globalCentroids);
	fc.startWorkers();

	fc.printHeader();
	cerr << "Init Time: " << duration_cast<milliseconds>(system_clock::now() - start).count() << "ms\n";


	cerr << "Import Data\n";
	auto start2 = system_clock::now();
	auto strdata = new fdd<string>(fc, argv[1]);
	cerr << "  Read Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";

	cerr << "Convert to Adjacency List\n";
	auto data = strdata->map<int, vector<int>>(&toVector)->cache();
	fc.updateInfo();
	numItems = data->getSize();
	cerr << numItems << " items" << '\n';

	cerr << "Init K-Means\n";
	vector<int> initialCentroidKeys;
	vector<vector<int>> initialCentroidPositions;
	for ( int i = 0; i < numCentroids; i++ ){
		initialCentroidKeys[i] = i;
		initialCentroidPositions[i].resize(2);
		initialCentroidPositions[i][0] = i % (numCentroids/3) ;
		initialCentroidPositions[i][1] = i / (numCentroids/3) ;
	}

	auto centroids = new indexedFdd<int, vector<int>> (fc, initialCentroidKeys.data(), initialCentroidPositions.data(), numCentroids);
	auto errors = new indexedFdd<int, float>(fc,numCentroids);

	// Figue out where
	data->update(&updateCentroidAssignment);
	auto iterationData = data->cogroup(centroids, errors);
	double error = 1000;
	fc.updateInfo();

	cerr << "Process Data\n";
	int i = 0;
	while( i < 10 ){
		cerr << "\033[1;32mIteration " << i++ << "\033[0m\n" ;
		start2 = system_clock::now();
		// Update Centroids positions
		iterationData->updateByKey(&updateCentroids);
		fc.updateInfo();
		
		// Update item centroid assignment
		data->update(&updateCentroidAssignment);
		fc.updateInfo();

		// Retrieve centroids
		auto newCentroids = centroids->collect();
		for ( int i = 0; i < numCentroids; i++ ){
			int & key = newCentroids[i].first;
			globalCentroids[key][0] = newCentroids[i].second[0];
			globalCentroids[key][1] = newCentroids[i].second[1];
		}
		fc.updateInfo();

		error = errors->reduce(&maxError).second;
		cerr << "  Error " << error << " time:" << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";
	}

}
