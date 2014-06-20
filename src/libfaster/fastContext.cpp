#include <fstream>

#include "worker.h"
#include "fastContext.h"

// Create a context with local as master
fastContext::fastContext(const fastSettings & s, void **& ft){

	funcTable = ft;
	settings = new fastSettings(s);
	comm = new fastComm( s.getMaster() );
	numFDDs = 0;
	numTasks = 0;

	// Create a Worker context and exit after finished
	if ( ! comm->isDriver() ){
		// Start worker role
		worker worker(comm, ft);

		worker.run();

		// Clean process
		delete comm; 
		delete settings; 
		exit(0);
	}// */
}


fastContext::~fastContext(){ 
	// Clean process
	fddList.clear();
	delete comm;
	delete settings;
}


unsigned long int fastContext::createFDD(size_t typeCode){
	
	fddType type = decodeType(typeCode);
	comm->sendCreateFDD(numFDDs, type);

	std::cerr << "    S:CreateFdd " << numFDDs << " " << (int) type << '\n';
	
	return numFDDs++;
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

unsigned long int fastContext::readFDD(const char * fileName){
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

unsigned long int fastContext::enqueueTask(fddOpType opT, unsigned long int idSrc, unsigned long int idRes, int funcId){
	fastTask * newTask = new fastTask();
	newTask->id = numTasks++;
	newTask->srcFDD = idSrc;
	newTask->destFDD = idRes;
	newTask->operationType = opT;

	// TODO do this later on a shceduler?
	comm->sendTask(newTask);
	std::cerr << "    S:Task" << newTask->id << '\n';

	return newTask->id;
}


