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

unsigned long int fastContext::createFDD(char){
	//send create fdd n. numFdds;
	return numFDDs++;
}

unsigned long int fastContext::readFDD(const char * fileName){
	//send read fdd n. numFdds from file fileName
	return numFDDs++;
}

// Propagate FDD destruction to other machines
void fastContext::destroyFDD(unsigned long int id){
	//send destroy fdd n. id;
}

unsigned long int fastContext::enqueueTask(fddOpType opT, unsigned long int idSrc, unsigned long int idRes, int funcId){
	fastTask * newTask = new fastTask();
	newTask->id = numTasks++;
	newTask->srcFDD = idSrc;
	newTask->destFDD = idRes;
	newTask->operationType = opT;

	comm->sendTask(newTask);

	return newTask->id;
}


