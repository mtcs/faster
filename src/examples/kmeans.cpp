#include <iostream>
#include <algorithm>
#include "libfaster.h"

using namespace std;
using namespace faster;

using std::chrono::system_clock;
using std::chrono::duration_cast;
using std::chrono::milliseconds;

int numCentroids = 10;
int numDims = 2;
//vector<float> globalCentroidsX;
//vector<float> globalCentroidsY;
//float * globalCentroidsX = NULL;
//float * globalCentroidsY = NULL;
vector<float> globalCentroids;

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

pair<int,vector<float>> initAssignment(const int & key UNUSED, vector<float> & position){
	//pair<int,vector<float>> p = {key % numCentroids, position};
	//return p;
	return make_pair(0, position);
}

float vecDistanceToCentroids(vector<float> &a, int i){
	//vector<float> dist(numDims);
	float dist = 0;
	for ( int d = 0; d < numDims ; d++ ){
		dist += pow(a[d] - globalCentroids[i*numDims + d], 2);
	}
	return (sqrt(dist));
}

float vecDistance(vector<float> &a, vector<float> &b){
	float dist = 0;
	for ( int d = 0; d < numDims ; d++ ){
		dist += pow(a[d] - b[d], 2);
	}
	return (sqrt(dist));
}

float vecDistance(float aX, float aY, float bX, float bY){
	int dx = aX - bX;
	int dy = aY - bY;
	return (sqrt(dx*dx + dy*dy));
}

void updateAssignment(int & key UNUSED, std::vector<float> & position){
	float minDistance = vecDistanceToCentroids(position, 0);

	for ( int i = 1; i < numCentroids; i++ ){
		float dist = vecDistanceToCentroids(position, i);
		if ( minDistance > dist ){
			key = i;
			minDistance = dist;
		}
	}
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

	vector<float> newPos (numDims, 0);
	for (size_t i = 0; i < items.size(); i++){
		vector<float> * item = (vector<float> *) items[i];
		for ( int d = 0; d < numDims ; d++ ){
			newPos[d] += (*item)[d];
		}
	}
	for ( int d = 0; d < numDims ; d++ ){
		newPos[d] /= items.size();
	}
	*error = vecDistance( *centroidPos, newPos);

	(*centroidPos) = newPos;

	//std::cerr  << "K:" << key << " i:" << items.size() << " C:" << centroidPosP.size() << " E:" << errorP.size() << "\n\n";
	//cerr << "   K:" << key <<  " O:"<< (*centroidPos)[0] << "-" << (*centroidPos)[0] << "  N:" << newPos[0] << "-" << newPos[1]<< "\n\n";
}

pair<int,vector<float>> getMaxDimSizes(const int & keyA UNUSED, vector<float> & a, const int & keyB UNUSED, vector<float> & b){
	vector<float> v(numDims);

	for ( int d = 0; d < numDims ; d++ ){
		v[d] = max(a[d], b[d]);
	}

	return make_pair(0, v);
}

pair<int,vector<float>> getMinDimSizes(const int & keyA UNUSED, vector<float> & a, const int & keyB UNUSED, vector<float> & b){
	vector<float> v(numDims);
	for ( int d = 0; d < numDims ; d++ ){
		v[d] = min(a[d], b[d]);
	}
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
		for ( int d = 0; d < numDims; d++ ){
			globalCentroids[key*numDims + d] = newCentroids[i].second[d];
		}
	}
}

float randomizeEmptyCentroids(unordered_map<int, size_t> & clusterSizes, vector<float> minP, vector<float> maxP){
	float error = 0;
	for ( int i = 0; i < numCentroids; i++ ){
		auto count = clusterSizes.find(i);
		if (count == clusterSizes.end()){
			vector<float> newPos(numDims);
			for ( int d = 0; d < numDims; d++ ){
				newPos[d] = (maxP[d] * rand()/(RAND_MAX + 1.0) ) + minP[d];
			}
			error = max(error, vecDistanceToCentroids(newPos, i));
			for ( int d = 0; d < numDims; d++ ){
				globalCentroids[i*numDims + d] = newPos[d];
			}
		}
	}
	return error;
}

pair<vector<int>, vector<vector<float>>> initGlobalCentroids(vector<float> & minV, vector<float> & maxV){
	vector<int> initialCentroidKeys(numCentroids);
	vector<vector<float>> initialCentroidPositions(numCentroids);


	for ( int i = 0; i < numCentroids; i++ ){
		initialCentroidKeys[i] = i;
		initialCentroidPositions[i].resize(numDims);
		for ( int d = 0; d < numDims ; d++ ){
			initialCentroidPositions[i][d] = (maxV[d] * rand()/(RAND_MAX + 1.0) ) + minV[d] ;
			globalCentroids[i*numDims + d] = initialCentroidPositions[i][d];
		}
	}

	return make_pair(initialCentroidKeys, initialCentroidPositions);
}

int main(int argc, char ** argv){
	int numItems;
	// Init Faster Framework
	auto start = system_clock::now();

	if (argc >= 3) numDims = atoi(argv[1]);
	if (argc >= 4) numCentroids = atoi(argv[2]);
	globalCentroids.resize(numCentroids * numDims);
	printf("   GlobalCentroids Size:%ld\n", globalCentroids.size());

	fastContext fc(argc, argv);

	fc.registerFunction((void*) &toVector, "toVectors");
	fc.registerFunction((void*) &initAssignment, "initAssignment");
	fc.registerFunction((void*) &updateAssignment, "updateAssignmnt");
	fc.registerFunction((void*) &updateCentroids, "updateCentroids");
	fc.registerFunction((void*) &maxError, "maxError");
	fc.registerFunction((void*) &getMaxDimSizes, "getMaxDimSizes");
	fc.registerFunction((void*) &getMinDimSizes, "getMinDimSizes");

	fc.registerGlobal(&globalCentroids);
	fc.startWorkers();
	if (!fc.isDriver())
		return 0;

	cerr << "------------ K-MEANS -------------\n";

	fc.printHeader();
	cerr << "Init Time: " << duration_cast<milliseconds>(system_clock::now() - start).count() << "ms\n";

	cerr << "Import Data\n";
	auto start2 = system_clock::now();
	auto strdata = new fdd<string>(fc, argv[3]);
	cerr << "  Read Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";

	cerr << "Convert to Vector\n";
	auto data = strdata->map(&toVector)->cache();
	numItems = data->getSize();
	fc.updateInfo();
	cerr << numItems << " items" << '\n';

	cerr << "\033[1;33mInit K-Means (" << numCentroids << " centroids, " << numDims << " dimensions)\033[0m\n";
	vector<float> maxV = data->reduce(&getMaxDimSizes).second;
	vector<float> minV = data->reduce(&getMinDimSizes).second;
	//cerr << "   X:[" << min[0] << " " << max[0] << "]\n";
	//cerr << "   X:[" << min[1] << " " << max[1] << "]\n";
	for ( int d = 0; d < numDims ; d++ ){
		maxV[d] -= minV[d];
	}
	auto clusterAssign = data->map(initAssignment)->cache();

	auto initialCentroids = initGlobalCentroids(minV, maxV);

	cerr << "\033[1;33m   Import Centroid Data\033[0m\n";
	auto centroidPos = new indexedFdd<int, vector<float>> (fc, initialCentroids.first.data(), initialCentroids.second.data(), numCentroids);
	centroidPos->cache();

	cerr << "\033[1;33m   Calculate Distances\033[0m\n";
	vector<float> initialCentroidError(numCentroids, 0);
	auto errors = new indexedFdd<int, float>(fc, initialCentroids.first.data(), initialCentroidError.data(), numCentroids);
	errors->cache();
	fc.updateInfo();

	// Join datasets
	cerr << "    \033[1;33mCogroup\033[0m\n" ;
	auto iterationData = centroidPos->cogroup(clusterAssign, errors)->cache();
	fc.updateInfo();

	//cerr << "\033[1;33m   Update Cluster Assignment\033[0m\n";
	//clusterAssign = clusterAssign->map(&updateAssignment)->cache();

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
	float error = 1000;
	fc.updateInfo();

	int i = 0;
	while( i < 10 ){
		//cerr << "\033[1;32mIteration " << i++ << "\033[0m\n" ;
		start2 = system_clock::now();

		// Update item centroid assignment
		//cerr << "    \033[1;33mUpdate Cluster Assignment (" << clusterAssign->getId() << ")\033[0m\n" ;
		clusterAssign->update(&updateAssignment);
		clusterAssign->setGroupedByKey(false);
		clusterAssign->groupByKey();
		fc.updateInfo();

		// Update Centroids positions
		//cerr << "    \033[1;33mUpdate Centroids\033[0m\n" ;
		iterationData->updateByKey(&updateCentroids);
		//fc.updateInfo();

		// Retrieve centroids
		//cerr << "    \033[1;33mReceive Centroids\033[0m\n" ;
		retrieveCentroidPositions(centroidPos);
		//fc.updateInfo();

		//cerr << "    \033[1;33mCount Clusters\033[0m\n" ;
		auto clusterSizes = clusterAssign->countByKey();
		//fc.updateInfo();
		error = max( error, randomizeEmptyCentroids(clusterSizes, minV, maxV) );

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

		error = errors->reduce(&maxError).second;
		fc.updateInfo();
		cerr << "  Error " << sqrt(error)
			<< " time:" << duration_cast<milliseconds>(system_clock::now() - start2).count()
			<< "ms\n";
	}

	start2 = system_clock::now();

	//clusterAssign->writeToFile(std::string("/tmp/clusterAssign"), std::string(".txt"));

	auto duration = duration_cast<milliseconds>(system_clock::now() - start).count();
	cerr << "  Write Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";
	cerr << "K-Means in " << clusterAssign->getSize()
		<< " points in "<< i
		<< " iterations! In " << duration
		<< "ms (error: " << sqrt(error)
		<<  ") \n";
}
