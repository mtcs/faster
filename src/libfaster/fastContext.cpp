#include <fstream>

#include "worker.h"
#include "fastContext.h"

// Create a context with local as master
fastContext::fastContext(const fastSettings & s){

	settings = new fastSettings(s);
	comm = new fastComm( s.getMaster() );
	numFDDs = 0;
	numTasks = 0;

}


fastContext::~fastContext(){ 
	// Tell workers to go home!
	std::cerr << "    S:CreateFdd ";
	comm->sendFinish();

	// Clean process
	fddList.clear();
	taskList.clear();
	delete comm;
	delete settings;
}

void fastContext::startWorkers(){
	// Create a Worker context and exit after finished
	if ( ! comm->isDriver() ){
		// Start worker role
		worker worker(comm, funcTable.data());

		worker.run();

		// Clean process
		delete comm; 
		delete settings; 
		exit(0);
	}// */

}

int fastContext::findFunc(void * funcP){
	//std::cerr << "  Find Function " << funcP ;
	for( size_t i = 0; i < funcTable.size(); ++i){
		if (funcTable[i] == funcP)
			return i;
	}
}

unsigned long int fastContext::createFDD(fddBase * ref, size_t typeCode, size_t size){
	
	fddType type = decodeType(typeCode);

	for (int i = 1; i < comm->numProcs; ++i){
		size_t dataPerProc = size / (comm->numProcs - 1);
		int rem = size % (comm->numProcs -1);
		if (i <= rem)
			dataPerProc += 1;
		comm->sendCreateFDD(numFDDs, type, dataPerProc, i);

		std::cerr << "    S:CreateFdd " << numFDDs << " " << (int) type << '\n';
	}
	fddList.insert(fddList.begin(), ref);
	
	return numFDDs++;
}
unsigned long int fastContext::createFDD(fddBase * ref, size_t typeCode){
	return createFDD(ref, typeCode, 0);
}

// Propagate FDD destruction to other machines
size_t findFileSize(const char* filename)
{
	std::ifstream in(filename, std::ifstream::in | std::ifstream::binary);
	in.seekg(0, std::ifstream::end);
	return in.tellg(); 
}

unsigned long int fastContext::readFDD(fddBase * ref, const char * fileName){
	//send read fdd n. numFdds from file fileName
	size_t fileSize = findFileSize(fileName);
	size_t offset = 0;

	for (int i = 1; i < comm->numProcs; ++i){
		size_t dataPerProc = fileSize / (comm->numProcs - 1);
		int rem = fileSize % (comm->numProcs -1);
		if (i <= rem)
			dataPerProc += 1;
		comm->sendReadFDDFile(numFDDs, std::string(fileName), dataPerProc, offset, i);
		offset += dataPerProc;
	}

	std::cerr << "    S:ReadFdd";
	comm->waitForReq(comm->numProcs - 1);
	std::cerr << '\n';
	
	return numFDDs++;
}

void fastContext::getFDDInfo(size_t & s){
	s = 0;
	for (int i = 1; i < comm->numProcs; ++i){
		size_t size;
		comm->recvFDDInfo(size);
		s += size;
	}
}

unsigned long int fastContext::enqueueTask(fddOpType opT, unsigned long int idSrc, unsigned long int idRes, int funcId){
	fastTask * newTask = new fastTask();
	newTask->id = numTasks++;
	newTask->srcFDD = idSrc;
	newTask->destFDD = idRes;
	newTask->operationType = opT;
	newTask->functionId = funcId;
	newTask->workersFinished = 0;

	// TODO do this later on a shceduler?
	comm->sendTask(*newTask);
	std::cerr << "    S:Task ID:" << newTask->id << " F:" << funcId<< '\n';

	taskList.insert(taskList.end(), newTask);

	return newTask->id;
}

void fastContext::recvTaskResult(unsigned long int &id, void * result, size_t & size){
	double time;

	comm->recvTaskResult(id, result, size, time);
	std::cerr << "    R:TaskResult " << id << "Result:"  << * (int*)result << '\n';

	taskList[id]->workersFinished++;
}





void fastContext::destroyFDD(unsigned long int id){
	comm->sendDestroyFDD(id);
}


