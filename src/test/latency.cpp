#include <iostream>
#include <algorithm>
#include <vector>
#include "libfaster.h"

using namespace std;
using namespace faster;

pair<int, int> map1(const int & k, int & input){
	return make_pair(k, input);
}

void bmap1(int * outk, int * out, int * k, int * input, size_t size ){
	#pragma omp parallel for
	for ( size_t i = 0; i < size; i++ ){
		outk[i] = k[i];
		out[i] = input[i];
	}
}

deque<pair<int, int>> fmap1(int k, int & input){
	deque<pair<int,int>> result;

	result.push_back(make_pair(k, input));

	return result;
}

void bfmap1(int *& outk, int *& out, size_t & outSize, int * k, int * input, size_t size ){
	outk = new int[size];
	out = new int[size];
	outSize = size;

	#pragma omp parallel for
	for ( size_t i = 0; i < size; i++ ){
		outk[i] = k[i];
		out[i] = input[i];
	}
}

pair<int, int> mapbk1(const int & k, vector<int*> & input){
	return make_pair(k, *( input[0] ));
}


pair<int, int>  reduce1(const int & ka, int &a, const int & kb, int &b){
	return make_pair(ka + kb, a + b);
}

pair<int, int>  breduce1(int * k, int * in, size_t size){
	int outk = k[0];
	int out = in[0];

	for ( size_t i = 1; i < size; i++ ){
		outk = outk + k[i];
		out = out + in[i];
	}

	return make_pair(outk, out);
}


pair<int, int> gmapbk1(const int & k, vector<void*> & input, vector<void*> & input2){
	if (input2.size() == 0)
		return make_pair(k, *( (int*) input.front() )) ;

	*((int*)input2.front()) = *((int*)input.front());
	return make_pair(k, *( (int*) input.front() ) + *( (int*) input2.front() ));
}

deque<pair<int, int>> gfmapbk1(const int & k, vector<void*> & input, vector<void*> & input2){
	deque<pair<int, int>> r;

	auto a = *((int*)input.front());

	if( input2.size() > 0 )
		*((int*)input2.front()) = a;

	r.push_back(make_pair(k, *( (int*) input.front() )));

	return r;
}

deque<pair<int, int>> gbfmap1(int * ka, void * a, size_t na, int * kb UNUSED, void * b, size_t nb){
	deque<pair<int, int>> r;

	if ( nb > 0 ){
		for ( size_t i = 0; (i < na) && (i < nb); i++ ){
			//((int*)b)[i] = ((int*)a)[i];
			r.push_back(make_pair(ka[i], ((int*)a)[i] + ((int*)b)[i]) );
		}
	}

	return r;
}

void gupdatebk1(const int & k, vector<void*> & input, vector<void*> & input2){
	if( input2.size() == 0 ) return;

	*( (int*) input2.front() ) = k + *( (int*) input.front() ) ;
}

void gbupdate1(int * ka, void * a, size_t na, int * kb, void * b, size_t nb){
	for ( size_t i = 0; (i < na) && (i < nb); i++ ){
		kb[i] = ka[i];
		((int*)b)[i] = ((int*)a)[i];
	}
}


int main(int argc, char ** argv){
	// Init Faster Framework
	cerr << "Init FastLib" << '\n';
	fastContext fc(argc,argv);

	fc.registerFunction((void*) &map1, "map");
	fc.registerFunction((void*) &bmap1, "bulkMap");
	fc.registerFunction((void*) &fmap1, "flatMap");
	fc.registerFunction((void*) &bfmap1, "bulkFlatMap");
	fc.registerFunction((void*) &mapbk1, "mapByKey");
	fc.registerFunction((void*) &reduce1, "reduce");
	fc.registerFunction((void*) &breduce1, "bulkReduce");
	fc.registerFunction((void*) &gmapbk1, "G.MapByKey");
	fc.registerFunction((void*) &gfmapbk1, "G.FlatMapByKey");
	fc.registerFunction((void*) &gbfmap1, "G.BulkFlatMap");
	fc.registerFunction((void*) &gupdatebk1, "G.UpdateByKey");
	fc.registerFunction((void*) &gbupdate1, "G.BulkUpdate");

	fc.startWorkers();

	int numItems = argc < 2 ? 100*1000 : atoi(argv[1]);
	int numRuns = argc < 3 ? 100 : atoi(argv[2]);


	cerr << "Generate Data\n" ;
	vector<int> rawdata(numItems);
	vector<int> rawkeys(numItems);

	for ( int i = 0; i < numItems; ++i ){
		rawkeys[i] = i;
		rawdata[i] = numItems - i;
	}

	cerr << "Import Data\n" ;
	auto data = new indexedFdd<int, int>(fc, rawkeys.data(), rawdata.data(), numItems);

	cerr << "Run tests on " << numItems << " size " << numRuns << " times\n";

	cerr << "Map\n" ;
	for ( int i = 0; i < numRuns; i++){
		data = data->map(map1);
	}
	//fc.updateInfo();

	cerr << "BulkMap\n" ;
	for ( int i = 0; i < numRuns; i++){
		data = data->bulkMap(bmap1);
	}
	//fc.updateInfo();

	cerr << "FlatMap\n" ;
	for ( int i = 0; i < numRuns; i++){
		data = data->flatMap(fmap1);
	}
	//fc.updateInfo();

	cerr << "BulkFlatMap\n" ;
	for ( int i = 0; i < numRuns; i++){
		data = data->bulkFlatMap(bfmap1);
	}
	//fc.updateInfo();

	vector<indexedFdd<int,int>*> dataV(numRuns);

	data->cache();

	cerr << "MapByKey" ;
	dataV[0] = data->mapByKey(mapbk1)->cache();
	for ( int i = 1; i < numRuns; i++){
		dataV[i] = dataV[i - 1]->mapByKey(mapbk1)->cache();
		cerr << "." ;
	}
	cerr << "\n" ;
	//fc.updateInfo();

	cerr << "Reduce\n" ;
	for ( int i = 0; i < numRuns; i++){
		pair<int,int> r UNUSED = dataV[i]->reduce(reduce1);
	}
	//fc.updateInfo();

	cerr << "BulkReduce\n" ;
	for ( int i = 0; i < numRuns; i++){
		pair<int,int> r UNUSED = dataV[i]->bulkReduce(breduce1);
	}

	//fc.updateInfo();
	cerr << "---- Grouped Functions ----\n" ;
	//cerr << "\033[0;33mPRESS ENTER TO CONTINUE\033[0m\n";
	//cin.get();

	cerr << "Cogroup" ;
	vector<groupedFdd<int> *> groupV(numRuns);
	for ( int i = 0; i < numRuns; i++){
		groupV[i] = data->cogroup(dataV[i]);
		cerr << "." ;
	}
	cerr << "\n" ;

	//fc.updateInfo();
	//cerr << "\033[0;33mPRESS ENTER TO CONTINUE\033[0m\n";
	//cin.get();


	cerr << "G.MapByKey" ;
	for ( int i = 0; i < numRuns; i++){
		auto result UNUSED = groupV[i]->mapByKey(gmapbk1);
		//groupV[i]->discard();
		result->discard();
		dataV[i]->discard();
		cerr << "." ;
	}

	cerr << "\n" ;
	//fc.updateInfo();
	//cerr << "\033[0;33mPRESS ENTER TO CONTINUE\033[0m\n";
	//cin.get();


	cerr << "Recreate Data" ;
	data->discard();
	data = new indexedFdd<int,int>(fc, rawkeys.data(), rawdata.data(), numItems);
	for ( int i = 0; i < numRuns; i++){
		dataV[i] = new indexedFdd<int,int>(fc, rawkeys.data(), rawdata.data(), numItems);
		dataV[i]->cache();
		cerr << "." ;
	}
	cerr << "\n" ;

	cerr << "Cogroup" ;
	for ( int i = 0; i < numRuns; i++){
		groupV[i] = data->cogroup(dataV[i])->cache();
		cerr << "." ;
	}
	cerr << "\n" ;
	//fc.updateInfo();


	//cerr << "TEST" ;
	//for ( int i = 0; i < numRuns; i++){
		//dataV[i]->map(map1);
	//}
	//fc.updateInfo();


	//cerr << "\033[0;33mPRESS ENTER TO CONTINUE\033[0m\n";
	//cin.get();
	cerr << "G.FlatMapByKey" ;
	for ( int i = 0; i < numRuns; i++){
		auto result = groupV[i]->flatMapByKey(gfmapbk1);
		result->discard();
		cerr << "." ;
	}
	cerr << "\n" ;
	//fc.updateInfo();
	//cerr << "\033[0;31mPRESS ENTER TO EXIT\033[0m\n";
	//cin.get();

	cerr << "G.BulkFlatMap" ;
	for ( int i = 0; i < numRuns; i++){
		auto result UNUSED = groupV[i]->bulkFlatMap(gbfmap1);
		result->discard();
		cerr << "." ;
	}
	cerr << "\n" ;
	//fc.updateInfo();

	cerr << "Recreate Data" ;
	for ( int i = 0; i < numRuns; i++){
		groupV[i]->discard();
		dataV[i]->discard();
	}
	data->discard();
	data = new indexedFdd<int,int>(fc, rawkeys.data(), rawdata.data(), numItems);
	for ( int i = 0; i < numRuns; i++){
		dataV[i] = new indexedFdd<int,int>(fc, rawkeys.data(), rawdata.data(), numItems);
		dataV[i]->cache();
		cerr << "." ;
	}
	cerr << "\n" ;

	cerr << "Cogroup" ;
	for ( int i = 0; i < numRuns; i++){
		groupV[i] = data->cogroup(dataV[i])->cache();
		cerr << "." ;
	}
	cerr << "\n" ;
	//fc.updateInfo();

	cerr << "G.UpdateByKey" ;
	for ( int i = 0; i < numRuns; i++){
		groupV[i]->updateByKey(gupdatebk1);
		cerr << "." ;
	}
	cerr << "\n" ;
	//fc.updateInfo();

	cerr << "G.BulkUpdate" ;
	for ( int i = 0; i < numRuns; i++){
		groupV[i]->bulkUpdate(gbupdate1);
		cerr << "." ;
	}
	cerr << "\n" ;
	for ( int i = 0; i < numRuns; i++){
		groupV[i]->discard();
	}
	//fc.updateInfo();

	fc.printInfo();

	//cerr << "\033[0;31mPRESS ENTER TO EXIT\033[0m\n";
	//cin.get();

	return 0;
}
