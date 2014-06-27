#include <string>
#include <fstream>
#include <iostream>

#include "fastComm.h"
#include "worker.h"

worker::worker(fastComm * c, void ** ft){
	std::cerr << "  Starting Worker" << '\n';
	funcTable = ft;
	comm = c;
	finished = false;
}

worker::~worker(){
}

void worker::run(){
	std::cerr << "  Worker Working..." << '\n';
	while (! finished ){
		int tag;
		fastTask task;
		unsigned long int id;
		fddType type;
		void * data;
		void ** data2D;
		size_t * lineSizes;
		size_t size, offset;
		std::string name;

		// Wait for a message to arrive
		comm->probeMsgs(tag);

		switch(tag){
			case MSG_TASK:
				comm->recvTask(task);
				std::cerr << "    R:Task ID:" << task.id << " F:" << task.functionId << " ";
				solve(task); // TODO Separate in a different thread ?
				std::cerr << ".\n";
				break;

			case MSG_CREATEFDD:
				comm->recvCreateFDD(id, type, size);
				std::cerr << "    R:CreateFdd ID:" << id << " T:" << (int) type << " S:" << size << "B";
				createFDD(id, type, size);
				std::cerr << ".\n";
				break;

			case MSG_DESTROYFDD:
				comm->recvDestroyFDD(id);
				std::cerr << "    R:DestroyFdd " << id;
				destroyFDD(id);
				std::cerr << ".\n";
				break;

			case MSG_FDDSETDATAID:
				comm->recvFDDSetData(id, data, size);
				std::cerr << "    R:SetFddData " << id << ' ' << size;
				setFDDData(id, data, size);
				std::cerr << ".\n";
				break;

			case MSG_FDDSET2DDATAID:
				comm->recvFDDSetData(id, data2D, lineSizes, size);
				std::cerr << "    R:SetFddData " << id << ' ' << size;
				setFDDData(id, data2D, lineSizes, size);
				std::cerr << ".\n";
				break;

			case MSG_READFDDFILE:
				comm->recvReadFDDFile(id, name, size, offset);
				std::cerr << "    R:ReadFddFile " << id <<" F:" << name<< "(offset:" << offset << ")" ;
				readFDDFile(id, name, size, offset);
				std::cerr << ".\n";
				break;

			case MSG_COLLECT:
				comm->recvCollect(id);
				std::cerr << "    R:Collect " << id;
				getFDDData(id, data, size);
				comm->sendFDDData(id, 0, data, size);
				std::cerr << ".\n";
				break;

			case MSG_FINISH:
				comm->recvFinish();
				std::cerr << "    R:FINISH ";
				finished = true;
				std::cerr << ".\n";
				break;
			default:
				break;
		}
	}
	std::cerr << "  DONE" << '\n';
}

template <typename T> workerFdd<T> * decodeFDD(workerFddBase * fddBase){
	if (fddBase->type == Float){
		return (workerFdd<float> *) fddBase;
	}
	return (workerFdd<int> *) fddBase;
}

void worker::createFDD (unsigned long int id, fddType type, size_t size){
	workerFddBase * newFdd;
	switch (type){
		case Null:
			break;
		case Char: 
			newFdd = new workerFdd<char>(id, type, size);
			break;
		case Int: 
			newFdd = new workerFdd<int>(id, type, size);
			break;
		case LongInt:
			newFdd = new workerFdd<long int>(id, type, size);
			break;
		case Float:
			newFdd = new workerFdd<float>(id, type, size);
			break;
		case Double:
			newFdd = new workerFdd<double>(id, type, size);
			break;
		case CharP: 
			newFdd = new workerFdd<char *>(id, type, size);
			break;
		case IntP: 
			newFdd = new workerFdd<int *>(id, type, size);
			break;
		case LongIntP:
			newFdd = new workerFdd<long int *>(id, type, size);
			break;
		case FloatP:
			newFdd = new workerFdd<float *>(id, type, size);
			break;
		case DoubleP:
			newFdd = new workerFdd<double *>(id, type, size);
			break;
		case Custom:
			//newFdd = new workerFdd<void *>(id, type, size);
	 		break;
		case String:
			newFdd = new workerFdd<std::string>(id, type, size);
	 		break;
		//default:
		//	newFdd = new workerFdd<int>(id, type, size);
		//	break;
	}
	fddList.insert(fddList.end(), newFdd);
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
