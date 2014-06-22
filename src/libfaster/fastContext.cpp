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

unsigned long int fastContext::createFDD(fddBase * ref, size_t typeCode, size_t size){
	
	fddType type = decodeType(typeCode);
	comm->sendCreateFDD(numFDDs, type, size/(comm->numProcs-1));

	std::cerr << "    S:CreateFdd " << numFDDs << " " << (int) type << '\n';
	fddList.insert(fddList.begin(), ref);
	
	return numFDDs++;
}
unsigned long int fastContext::createFDD(fddBase * ref, size_t typeCode){
	return createFDD(ref, typeCode, 0);
}

// Propagate FDD destruction to other machines
void fastContext::destroyFDD(unsigned long int id){
	comm->sendDestroyFDD(id);
}

size_t findFileSize(const char* filename)
{
	std::ifstream in(filename, std::ifstream::in | std::ifstream::binary);
	in.seekg(0, std::ifstream::end);
	return in.tellg(); 
}

unsigned long int fastContext::readFDD(fddBase * ref, const char * fileName){
	//send read fdd n. numFdds from file fileName
	size_t fileSize = findFileSize(fileName);
	size_t dataPerProc = fileSize / (comm->numProcs - 1);

	for (int i = 1; i < comm->numProcs; ++i){
		comm->sendReadFDDFile(numFDDs, std::string(fileName), dataPerProc, i * dataPerProc, i);
	}

	std::cerr << "    S:ReadFdd";
	comm->waitForReq(comm->numProcs - 1);
	std::cerr << '\n';
	
	return numFDDs++;
}

void fastContext::getFDDInfo(size_t & s){
	size_t size, sum = 0;

	for (int i = 1; i < comm->numProcs; ++i){
		comm->recvFDDInfo(size);
		sum += size;
	}

	s = sum;
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


