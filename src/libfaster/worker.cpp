#include <string>
#include <fstream>
#include <iostream>

#include "fastComm.h"
#include "worker.h"

worker::worker(fastComm * c, void **& ft){
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

			case MSG_READFDDFILE:
				comm->recvReadFDDFile(id, name, size, offset);
				std::cerr << "    R:ReadFddFile " << id << name;
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
		case String:
			newFdd = new workerFdd<std::string>(id, type, size);
	 		break;
		case Custom:
			//newFdd = new workerFdd<void *>(id, type, size);
	 		break;
		case Null:
			break;
		default:
			newFdd = new workerFdd<int>(id, type, size);
			break;
	}
	fddList.insert(fddList.end(), newFdd);
}


void worker::destroyFDD(unsigned long int id){
	delete fddList[id];
	fddList[id] = NULL;
}

void worker::setFDDData(unsigned long int id, void * data, size_t size){
	workerFddBase * fdd = fddList[id];

	if (fdd == NULL) { std::cerr << "ERROR: Could not find FDD!"; exit(201); }

	fdd->setData( data, size );
}

void worker::getFDDData(unsigned long int id, void *& data, size_t &size){
	workerFddBase * fdd = fddList[id];

	if (fdd == NULL) { std::cerr << "ERROR: Could not find FDD!"; exit(201); }

	data = fdd->getData();
	size = fdd->getSize();
}

void worker::readFDDFile(unsigned long int id, std::string &filename, size_t size, size_t offset){

	workerFdd<std::string> * newFdd = new workerFdd<std::string>(id, String);
	fddList.insert(fddList.end(), newFdd);

	// TODO Treat other kinds of input files
	std::ifstream inFile(filename, std::ifstream::in);
	inFile.seekg(offset - 1, inFile.beg);
	
	char c = inFile.get();
	std::string line; 
	std::getline( inFile, line ); 
	
	// If the other process doesn't have this line get it
	if ( c == '\n' ){
		newFdd->insert(line);
	}
	
	while( size_t(inFile.tellg()) < (offset + size) ){
		std::getline( inFile, line ); 

		newFdd->insert(line);
	}
	newFdd->shrink();

	std::cerr << "    S:FDDInfo ";
	comm->sendFDDInfo(newFdd->getSize());

}






template <typename T, typename U>
void worker::apply(fastTask &task, workerFdd<U> * dest, workerFdd<T> * src){
	T result;
	switch (task.operationType){
		case Map:
			src->map(*dest, ( U (*)(T &) ) funcTable[task.functionId]);
			result = 0;
			std::cerr << " S:RESULT Map ";
			comm->sendTaskResult(task.id, &result, sizeof(char), 0);
			break;
		case Reduce:
			src->reduce(result, ( T (*)(T&, T&) ) funcTable[task.functionId]);
			std::cerr << " S:RESULT Reduce " << result;
			comm->sendTaskResult(task.id, &result, sizeof(T), 0);
			break;
	}
}

template <typename T>
void worker::preapply(fastTask &task, workerFdd<T> * dest){
	workerFddBase * src = fddList[task.srcFDD];

	if (src->getType() == Int){
		workerFdd<int> * srcFDD = (workerFdd<int> *) src;
		apply(task, dest, srcFDD);
	}
}

void worker::solve(fastTask &task){

	if (task.operationType == Reduce){
		preapply<int>(task, NULL);
	}else{
		workerFddBase * fdd = fddList[task.destFDD];
		if (fdd->getType() == Int){
			workerFdd<int> * destFDD = (workerFdd<int> *) fdd;
			preapply(task, destFDD);
		}
	}
}
