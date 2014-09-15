#include <string>
#include <fstream>
#include <iostream>

#include "fastComm.h"
#include "fastCommBuffer.h"
#include "workerFdd.h"
#include "worker.h"

faster::worker::worker(fastComm * c, void ** ft){
	std::cerr << "  Starting Worker " << c->getProcId() << '\n';
	funcTable = ft;
	comm = c;
	finished = false;
	fddList.reserve(50);
}

faster::worker::~worker(){
}


void faster::worker::discardFDD(unsigned long int id){
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




void faster::worker::solve(fastTask &task){
	workerFddBase * src = fddList[task.srcFDD];
	workerFddBase * dest = NULL;

	if (src == NULL) { std::cerr << "\nERROR: Could not find FDD!"; exit(201); }

	if ( task.operationType & OP_GENERICMAP)
		dest = fddList[task.destFDD];

	if ( task.functionId != -1  )
		src->preapply(task.id, funcTable[task.functionId], task.operationType, dest, comm);
	else
		src->preapply(task.id, NULL, task.operationType, dest, comm);
}

void faster::worker::collect(unsigned long int id){
	workerFddBase * fdd = fddList[id];

	if (fdd == NULL) { std::cerr << "\nERROR: Could not find FDD!"; exit(201); }

	fdd->collect(comm);
}
