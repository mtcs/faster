#include <string>
#include <fstream>
#include <iostream>
#include <chrono>

#include "fastComm.h"
#include "fastCommBuffer.h"
#include "workerFdd.h"
#include "worker.h"

typedef std::chrono::milliseconds milli;
typedef std::chrono::system_clock sysClock;

faster::worker::worker(fastComm * c, void ** ft){
	std::cerr << "  Starting Worker " << c->getProcId() << '\n';
	funcTable = ft;
	comm = c;
	finished = false;
}

faster::worker::~worker(){
}


void faster::worker::destroyFDD(unsigned long int id){
	delete fddList[id];
	fddList[id] = NULL;
}

void faster::worker::setFDDData(unsigned long int id, void * data, size_t size){
	workerFddBase * fdd = fddList[id];

	if (fdd == NULL) { std::cerr << "\nERROR: Could not find FDD!"; exit(201); }

	fdd->setDataRaw( data, size );
}
void faster::worker::setFDDIData(unsigned long int id, void * keys, void * data, size_t size){
	workerFddBase * fdd = fddList[id];

	if (fdd == NULL) { std::cerr << "\nERROR: Could not find FDD!"; exit(201); }

	fdd->setDataRaw( keys, data, size );
}

void faster::worker::setFDDData(unsigned long int id, void * data, size_t * lineSizes, size_t size){
	workerFddBase * fdd = fddList[id];

	if (fdd == NULL) { std::cerr << "\nERROR: Could not find FDD!"; exit(201); }

	fdd->setDataRaw( data, lineSizes, size );
}

void faster::worker::setFDDIData(unsigned long int id, void * keys, void * data, size_t * lineSizes, size_t size){
	workerFddBase * fdd = fddList[id];

	if (fdd == NULL) { std::cerr << "\nERROR: Could not find FDD!"; exit(201); }

	fdd->setDataRaw( keys, data, lineSizes, size );
}

/*void faster::worker::getFDDData(unsigned long int id, void *& data, size_t &size){
	workerFddBase * fdd = fddList[id];

	if (fdd == NULL) { std::cerr << "\nERROR: Could not find FDD!"; exit(201); }

	data = fdd->getData();
	size = fdd->getSize();
}*/




void faster::worker::preapply(fastTask &task, workerFddBase * destFDD){
	using std::chrono::system_clock;
	using std::chrono::duration_cast;
	using std::chrono::milliseconds;

	workerFddBase * src = fddList[task.srcFDD];
	size_t rSize;
	char r = 0;
	void * result;

	auto start = system_clock::now();
	src->apply(funcTable[task.functionId], task.operationType, destFDD, result, rSize);
	auto end = system_clock::now();

	auto duration = duration_cast<milliseconds>(end - start);


	std::cerr << " ET:" << duration.count() << " ";
	if (task.operationType & (OP_GENERICREDUCE)){
		comm->sendTaskResult(task.id, result, rSize, duration.count());
	}else{
		comm->sendTaskResult(task.id, &r, sizeof(char), duration.count());
	}

}


void faster::worker::solve(fastTask &task){

	if ( task.operationType & OP_GENERICREDUCE){
		// Don't consider the output fdd pointer
		preapply(task, NULL);
		return;
	}
	if ( task.operationType & OP_GENERICMAP){
		workerFddBase * fdd = fddList[task.destFDD];
		preapply(task, fdd);
		return;
	}
	if ( task.operationType == OP_GroupByKey){
		workerFddBase * fdd = fddList[task.srcFDD];
		fdd->groupByKey(comm);
		return;
	}
	if ( task.operationType == OP_CountByKey){
		workerFddBase * fdd = fddList[task.srcFDD];
		fdd->countByKey(comm);
		return;
	}
}

void faster::worker::collect(unsigned long int id){
	workerFddBase * fdd = fddList[id];

	if (fdd == NULL) { std::cerr << "\nERROR: Could not find FDD!"; exit(201); }

	fdd->collect(comm);
}
