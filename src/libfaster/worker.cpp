#include <string>
#include <fstream>
#include <iostream>

#include "fastComm.h"
#include "workerFdd.h"
#include "worker.h"

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
	std::cerr << " TaskListLength:" << fddList.size() << " " << task.srcFDD;
	workerFddBase * src = fddList[task.srcFDD];

	void * result ;
	if (destFDD)
		result = new char[destFDD->itemSize()];
	size_t rSize;
	char r = 0;

	src->apply(funcTable[task.functionId], task.operationType, destFDD, result, rSize);
	std::cerr << " S:RESULT ";
	if (task.operationType & (OP_GENERICREDUCE))
		comm->sendTaskResult(task.id, result, src->baseSize(), 0);
	else
		comm->sendTaskResult(task.id, &r, sizeof(char), 0);
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
