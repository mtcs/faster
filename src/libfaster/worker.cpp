#include <string>
#include <fstream>
#include <iostream>

#include "fastComm.h"
#include "workerFdd.h"
#include "workerIFdd.h"
#include "worker.h"

worker::worker(fastComm * c, void ** ft){
	std::cerr << "  Starting Worker" << '\n';
	funcTable = ft;
	comm = c;
	finished = false;
}

worker::~worker(){
}


template <typename T> workerFdd<T> * decodeFDD(workerFddBase * fddBase){
	if (fddBase->type == Float){
		return (workerFdd<float> *) fddBase;
	}
	return (workerFdd<int> *) fddBase;
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

void worker::readFDDFile(unsigned long int id, std::string &filename, size_t size, size_t offset){
	std::string line; 
	char c;

	workerFdd<std::string> * newFdd = new workerFdd<std::string>(id, String);

	if (newFdd == NULL) { std::cerr << "\nERROR: Could not find FDD!"; exit(201); }

	fddList.insert(fddList.end(), newFdd);

	// TODO Treat other kinds of input files
	std::ifstream inFile(filename, std::ifstream::in);

	if ( ! inFile.good() ){
		std::cerr << "\nERROR: Could not read input File " << filename << "\n";
		exit(202);
	}


	if( offset > 0){
		inFile.seekg(offset-1, inFile.beg);
		c = inFile.get();
		// If the other process doesn't have this line, get it!
		if ( c == '\n' ) {
			std::getline( inFile, line ); 
			newFdd->insert(line);
		}
	}
	
	// Start reading lines
	while( size_t(inFile.tellg()) < (offset + size) ){
		std::getline( inFile, line ); 

		newFdd->insert(line);
	}
	inFile.close();

	newFdd->shrink();

	std::cerr << "    S:FDDInfo ";
	comm->sendFDDInfo(newFdd->getSize());

}




void worker::preapply(fastTask &task, workerFddBase * destFDD){
	workerFddBase * src = fddList[task.srcFDD];

	void * result = new char[destFDD->itemSize()];
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
