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
				std::cerr << "    R:Task " << '\n';
				solve(task); // TODO Separate in a different thread ?
				break;

			case MSG_CREATEFDD:
				comm->recvCreateFDD(id, type);
				std::cerr << "    R:CreateFdd " << id << " " << (int) type << '\n';
				createFDD(id, type);
				break;

			case MSG_DESTROYFDD:
				comm->recvDestroyFDD(id);
				std::cerr << "    R:DestroyFdd " << id << '\n';
				destroyFDD(id);
				break;

			case MSG_FDDSETDATAID:
				comm->recvFDDSetData(id, data, size);
				std::cerr << "    R:SetFddData " << id << ' ' << size << '\n';
				setFDDData(id, data, size);
				break;

			case MSG_READFDDFILE:
				comm->recvReadFDDFile(id, name, size, offset);
				std::cerr << "    R:ReadFddFile " << id << name << '\n';
				readFDDFile(id, name, size, offset);
				break;

			case MSG_COLLECT:
				comm->recvCollect(id);
				std::cerr << "    R:Collect " << id << '\n';
				getFDDData(id, data, size);
				comm->sendFDDData(id, 0, data, size);
				break;

			case MSG_FINISH:
				comm->recvFinish();
				std::cerr << "    R:FINISH " << '\n';
				finished = true;
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

void worker::createFDD (unsigned long int id, fddType type){
	std::cerr << "      Create FDD: " << id << " " << (int) type << ' ';
	switch (type){
		case Int: {
			workerFdd<int> * newFdd = new workerFdd<int>(id, type);
			fddList.insert(fddList.end(), newFdd);
			std::cerr << "Int ";
			break;
		}
		case LongInt: {
			std::cerr << "LongInt ";
			break;
		}
		case Float: {
			std::cerr << "Float ";
			break;
		}
		case Double: {
			std::cerr << "Double ";
			break;
		}
		case String: {
			std::cerr << "String ";
	 		break;
		}
		case Custom: {
			std::cerr << "Custom ";
	 		break;
		}
		case Null: {
			std::cerr << "Null ";
			break;
		}
		default: {
			std::cerr << "Communication error!";
			break;
		}
	}
	std::cerr << "LIST SIZE: " << fddList.size() << '\n';
}

void worker::destroyFDD(unsigned long int id){
	delete fddList[id];
	fddList[id] = NULL;
}

void worker::setFDDData(unsigned long int id, void * data, size_t size){
	workerFddBase * fdd = fddList[id];

	if (fdd == NULL) return;

	if (fdd->getType() == Int){
		workerFdd<int> * decFdd = (workerFdd<int> *) fdd;
		decFdd->setData( (int *) data, size * sizeof(int) );
	}
}

void worker::getFDDData(unsigned long int id, void *& data, size_t &size){
	workerFddBase * fdd = fddList[id];

	if (fdd->getType() == Int){
		workerFdd<int> * decFdd = (workerFdd<int> *) fdd;
		data = decFdd->getData();
		size = decFdd->getSize();
	}
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
}






template <typename T, typename U>
void worker::apply(fastTask &task, workerFdd<U> * dest, workerFdd<T> * src){
	T result;
	switch (task.operationType){
		case Map:
			src->map(*dest, ( U (*)(T &) ) funcTable[task.functionId]);
			result = 0;
			break;
		case Reduce:
			src->reduce(result, ( T (*)(T&, T&) ) funcTable[task.functionId]);
			break;
	}
	comm->sendTaskResult(task.id, &result, sizeof(T), 0);
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
