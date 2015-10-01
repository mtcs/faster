#include <iostream>
#include <algorithm>
#include "libfaster.h"

using namespace std;
using namespace faster;

using std::chrono::system_clock;
using std::chrono::duration_cast;
using std::chrono::milliseconds;

int numCentroids = 10;
//vector<float> globalCentroidsX;
//vector<float> globalCentroidsY;
float * globalCentroidsX = NULL;
float * globalCentroidsY = NULL;

pair<int,vector<float>> toVector(string & input){
	
	stringstream ss(input);
	vector<float> position;
	position.reserve(2);
	int key;
	float val;

	ss >> key;

	while(ss >> val){
		position.insert(position.end(), val);
	}

	return make_pair(key, move(position));
}// */

pair<int,vector<float>> initAssignment(const int & key, vector<float> & position){
	return make_pair(key % numCentroids, position);
	//return make_pair(0, position);
}

float vecDistanceToCentroids(vector<float> &a, int i){
	int dx = a[0] - globalCentroidsX[i];
	int dy = a[1] - globalCentroidsY[i];
	return (dx*dx + dy*dy);
}

float vecDistance(vector<float> &a, vector<float> &b){
	int dx = a[0] - b[0];
	int dy = a[1] - b[1];
	return (dx*dx + dy*dy);
}

float vecDistance(float aX, float aY, float bX, float bY){
	int dx = aX - bX;
	int dy = aY - bY;
	return (dx*dx + dy*dy);
}

void updateAssignment(int & key, std::vector<float> & position){
	int closer = 0;

	//printf("   Centroids:%d\n", numCentroids);
	//printf("   Centroid[0]:%.1f-%.1f\n", globalCentroidsX[0], globalCentroidsY[0]);

	float minDistance = vecDistanceToCentroids(position, 0);
	for ( int i = 1; i < numCentroids; i++ ){
		float dist = vecDistanceToCentroids(position, i);
		if ( minDistance > dist ){
			closer = i;
			minDistance = dist;
		}
	}
	key = closer;
}

void updateCentroids(
		const int & key UNUSED, 
		std::vector<void*> & centroidPosP, 
		std::vector<void*> & items, 
		std::vector<void*> & errorP){
	//std::cerr  << "K:" << key << " i:" << items.size() << " C:" << centroidPosP.size() << " E:" << errorP.size() << "\n";
	if (items.size() == 0 ) return;

	vector<float> * centroidPos  = (vector<float> *) centroidPosP[0];
	float * error = (float *) errorP[0];

	vector<float> newPos (2, 0);
	for (size_t i = 0; i < items.size(); i++){
		vector<float> * item = (vector<float> *) items[i];
		newPos[0] += (*item)[0];
		newPos[1] += (*item)[1];
	}
	newPos[0] /= items.size();
	newPos[1] /= items.size();

	*error = vecDistance( *centroidPos, newPos);
	//cerr << "   K:" << key <<  " O:"<< (*centroidPos)[0] << "-" << (*centroidPos)[0] << "  N:" << newPos[0] << "-" << newPos[1]<< "\n";

	(*centroidPos) = newPos;
}

pair<int,vector<float>> getMaxXY(const int & keyA UNUSED, vector<float> & a, const int & keyB UNUSED, vector<float> & b){
	vector<float> v(2);
	v[0] = max(a[0], b[0]);
	v[1] = max(a[1], b[1]);
	return make_pair(0, move(v));
}

pair<int,vector<float>> getMinXY(const int & keyA UNUSED, vector<float> & a, const int & keyB UNUSED, vector<float> & b){
	vector<float> v(2);
	v[0] = min(a[0], b[0]);
	v[1] = min(a[1], b[1]);
	return make_pair(0, move(v));
}

pair<int,float> maxError(const int & keyA UNUSED, float & a, const int & keyB UNUSED, float & b){
	return make_pair(0, max(a,b));
}

void retrieveCentroidPositions(indexedFdd<int, vector<float>> * centroidPos){
	//cerr << "\033[1;33mUpdate Global Centroids\033[0m\n" ;
	auto newCentroids = centroidPos->collect();
	for ( int i = 0; i < numCentroids; i++ ){
		int & key = newCentroids[i].first;
		globalCentroidsX[key] = newCentroids[i].second[0];
		globalCentroidsY[key] = newCentroids[i].second[1];
	}
}

float randomizeEmptyCentroids(unordered_map<int, size_t> & clusterSizes, float minX, float maxX, float minY, float maxY){
	float error = 0;
	for ( int i = 0; i < numCentroids; i++ ){
		auto count = clusterSizes.find(i);
		if (count == clusterSizes.end()){
			float newX = (maxX * rand()/(RAND_MAX + 1.0) ) + minX;
			float newY = (maxY * rand()/(RAND_MAX + 1.0) ) + minY;
			error = max(error, vecDistance(newX, newY, globalCentroidsX[i], globalCentroidsY[i]));
			globalCentroidsX[i] = newX;
			globalCentroidsY[i] = newY;
		}
	}
	return error;
}

int main(int argc, char ** argv){
	int numItems;
	// Init Faster Framework
	auto start = system_clock::now();

	if (argc < 3) 
		numCentroids = 10 ; 
	else 
		numCentroids = atoi(argv[2]);


	fastContext fc(argc, argv);
	fc.registerFunction((void*) &toVector, "toVectors");
	fc.registerFunction((void*) &initAssignment, "initAssignment");
	fc.registerFunction((void*) &updateAssignment, "updateAssignmnt");
	fc.registerFunction((void*) &updateCentroids, "updateCentroids");
	fc.registerFunction((void*) &maxError, "maxError");
	fc.registerFunction((void*) &getMaxXY, "getMaxXY");
	fc.registerFunction((void*) &getMinXY, "getMinXY");

	fc.registerGlobal(&globalCentroidsX, numCentroids*sizeof(float));
	fc.registerGlobal(&globalCentroidsY, numCentroids*sizeof(float));
	fc.startWorkers();

	cerr << "------------ K-MEANS -------------\n";

	fc.printHeader();
	cerr << "Init Time: " << duration_cast<milliseconds>(system_clock::now() - start).count() << "ms\n";

	//globalCentroidsX.resize(numCentroids);
	//globalCentroidsY.resize(numCentroids);
	globalCentroidsX = new float[numCentroids];
	globalCentroidsY = new float[numCentroids];


	cerr << "Import Data\n";
	auto start2 = system_clock::now();
	auto strdata = new fdd<string>(fc, argv[1]);
	cerr << "  Read Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";

	cerr << "Convert to Vector\n";
	auto data = strdata->map(&toVector)->cache();
	numItems = data->getSize();
	fc.updateInfo();
	cerr << numItems << " items" << '\n';

	cerr << "\033[1;33mInit K-Means (" << numCentroids << " centroids)\033[0m\n";
	vector<float> maxXY = data->reduce(&getMaxXY).second;
	vector<float> minXY = data->reduce(&getMinXY).second;
	cerr << "   X:[" << minXY[0] << " " << maxXY[0] << "]\n";
	cerr << "   X:[" << minXY[1] << " " << maxXY[1] << "]\n";
	maxXY[0] -= minXY[0];
	maxXY[1] -= minXY[1];

	auto clusterAssign = data->map(initAssignment)->cache();
	vector<int> initialCentroidKeys(numCentroids);
	vector<float> initialCentroidError(numCentroids, 0);
	vector<vector<float>> initialCentroidPositions(numCentroids);
	for ( int i = 0; i < numCentroids; i++ ){
		initialCentroidKeys[i] = i;
		initialCentroidPositions[i].resize(2);
		initialCentroidPositions[i][0] = (maxXY[0] * rand()/(RAND_MAX + 1.0) ) + minXY[0] ;
		initialCentroidPositions[i][1] = (maxXY[1] * rand()/(RAND_MAX + 1.0) ) + minXY[1]  ;
		globalCentroidsX[i] = initialCentroidPositions[i][0];
		globalCentroidsY[i] = initialCentroidPositions[i][1];
	}

	cerr << "\033[1;33mInit K-Means Step2\033[0m\n";
	auto centroidPos = new indexedFdd<int, vector<float>> (fc, initialCentroidKeys.data(), initialCentroidPositions.data(), numCentroids);
	centroidPos->cache();

	cerr << "\033[1;33mInit K-Means Step3\033[0m\n";
	auto errors = new indexedFdd<int, float>(fc, initialCentroidKeys.data(), initialCentroidError.data(), numCentroids);
	errors->cache();

	cerr << "\033[1;33mInit K-Means Step4\033[0m\n";
	clusterAssign->update(&updateAssignment);
	
	// PRINT INFO
	/*for(size_t i = 0; i < numCentroids; i++) {
		printf("[%.1f ", globalCentroidsX[i]);
		printf("%.1f]  ", globalCentroidsY[i]);
	}
	cerr << "\n" ; // */
	//auto v = clusterAssign->collect();
	//sort(v.begin(), v.end());
	//for(size_t i = 0; i < v.size(); i++) printf("%d ", v[i].first);
	//cerr << "\n" ;
	cerr << "\033[1;33mInit K-Means Step5\033[0m\n";
	auto iterationData = centroidPos->cogroup(clusterAssign, errors)->cache();
	float error = 1000;
	fc.updateInfo();

	cerr << "Process Data\n";
	int i = 0;
	while( i < 10 ){
		cerr << "\033[1;32mIteration " << i++ << "\033[0m\n" ;
		start2 = system_clock::now();
		// Update Centroids positions
		clusterAssign->groupByKey();
		iterationData->updateByKey(&updateCentroids);
		fc.updateInfo();
		
		// Retrieve centroids
		retrieveCentroidPositions(centroidPos);
		fc.updateInfo();

		auto clusterSizes = clusterAssign->countByKey();
		error = max( error, randomizeEmptyCentroids(clusterSizes, minXY[0], maxXY[0], minXY[1], maxXY[1]) );
		
		// Update item centroid assignment
		clusterAssign->update(&updateAssignment);
		// PRINT INFO
		/*for(size_t i = 0; i < numCentroids; i++) {
			printf("[%.1f ", globalCentroidsX[i]);
			printf("%.1f]  ", globalCentroidsY[i]);
		}
		cerr << "\n" ;// */
		//v = clusterAssign->collect();
		//sort(v.begin(), v.end());
		//for(size_t i = 0; i < v.size(); i++) printf("%d ", v[i].first);
		//cerr << "\n" ;
		
		clusterAssign->setGroupedByKey(false);
		fc.updateInfo();


		error = errors->reduce(&maxError).second;
		cerr << "  Error " << sqrt(error) << " time:" << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";
	}

	start2 = system_clock::now();

	clusterAssign->writeToFile(std::string("/tmp/clusterAssign"), std::string(".txt"));

	auto duration = duration_cast<milliseconds>(system_clock::now() - start).count();
	cerr << "  Write Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";
	cerr << "K-Means in " << clusterAssign->getSize() << " points in "<< i << " iterations! In " << duration << "ms (error: " << sqrt(error) <<  ") \n";
}
