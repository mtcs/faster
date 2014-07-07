#include <string>
#include <fstream>
#include <iostream>
#include <chrono>

#include "fastComm.h"
#include "workerFdd.h"
#include "worker.h"

typedef std::chrono::milliseconds milli;
typedef std::chrono::system_clock sysClock;

worker::worker(fastComm * c, void ** ft){
	std::cerr << "  Starting Worker " << c->getProcId() << '\n';
	funcTable = ft;
	comm = c;
	finished = false;
}

worker::~worker(){
}


void worker::destroyFDD(unsigned long int id){
	delete fddList[id];
	fddList[id] = NULL;
}

void worker::setFDDData(unsigned long int id, void * data, size_t size){
	workerFddBase * fdd = fddList[id];

	if (fdd == NULL) { std::cerr << "\nERROR: Could not find FDD!"; exit(201); }

	fdd->setData( data, size );
}

void worker::setFDDData(unsigned long int id, void ** data, size_t * lineSizes, size_t size){
	workerFddBase * fdd = fddList[id];

	if (fdd == NULL) { std::cerr << "\nERROR: Could not find FDD!"; exit(201); }

	fdd->setData( data, lineSizes, size );
}

void worker::getFDDData(unsigned long int id, void *& data, size_t &size){
	workerFddBase * fdd = fddList[id];

	if (fdd == NULL) { std::cerr << "\nERROR: Could not find FDD!"; exit(201); }

	data = fdd->getData();
	size = fdd->getSize();
}




void worker::preapply(fastTask &task, workerFddBase * destFDD){
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

void worker::solve(fastTask &task){

	if ( task.operationType & OP_GENERICREDUCE){
		// Don't consider the output fdd pointer
		preapply(task, NULL);

	}else{
		workerFddBase * fdd = fddList[task.destFDD];
		preapply(task, fdd);
	}
}
