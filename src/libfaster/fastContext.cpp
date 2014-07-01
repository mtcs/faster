#include <fstream>

#include "worker.h"
#include "fastContext.h"
#include "misc.h"

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

void fastContext::registerFunction(void * funcP){
	//std::cerr << "  Register " << funcP ;
	funcTable.insert(funcTable.end(), funcP);
	//std::cerr << ".\n";
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

		std::cerr << "    S:CreateFdd ID:" << numFDDs << " T:" << (int) type << " S:" << dataPerProc << '\n';
	}
	fddList.insert(fddList.begin(), ref);
	
	return numFDDs++;
}
unsigned long int fastContext::createFDD(fddBase * ref, size_t typeCode){
	return createFDD(ref, typeCode, 0);
}

unsigned long int fastContext::createIFDD(fddBase * ref, size_t kTypeCode, size_t tTypeCode, size_t size){
	fddType kType = decodeType(kTypeCode);
	fddType tType = decodeType(tTypeCode);

	for (int i = 1; i < comm->numProcs; ++i){
		size_t dataPerProc = size / (comm->numProcs - 1);
		int rem = size % (comm->numProcs -1);
		if (i <= rem)
			dataPerProc += 1;
		comm->sendCreateIFDD(numFDDs, kType, tType, dataPerProc, i);

		std::cerr << "    S:CreateIFdd ID:" << numFDDs << " K:" << (int) kType << " T:" << (int) tType << " S:" << dataPerProc <<'\n';
	}
	fddList.insert(fddList.begin(), ref);
	return numFDDs++;
}
unsigned long int fastContext::createIFDD(fddBase * ref, size_t kTypeCode, size_t tTypeCode){
	return createIFDD(ref, kTypeCode, tTypeCode, 0);
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
	std::cerr << "    R:TaskResult ID:" << id << " Result:"  << * (int*)result << '\n';

	taskList[id]->workersFinished++;
}

template <typename T>
void fastContext::parallelize(unsigned long int id, T * data, size_t size){
	//int numBlocks = ceil ( size / settings->blockSize );
	//int blocksPerProc = numBlocks / (comm->numProcs - 1); // TODO DYNAMICALLY VARIATE BLOCK PER PROC LATER
	size_t offset = 0;

	for (int i = 1; i < comm->numProcs; ++i){
		int dataPerProc = size/(comm->numProcs - 1);
		int rem = size % (comm->numProcs -1);
		if (i <= rem)
			dataPerProc += 1;
		std::cerr << "    S:FDDSetData P" << i << " ID:" << id << " S:" << dataPerProc << "";

		comm->sendFDDSetData(id, i, &data[offset], dataPerProc * sizeof(T));
		offset += dataPerProc;
		std::cerr << ".\n";
	}
	comm->waitForReq(comm->numProcs - 1);
}
template void fastContext::parallelize(unsigned long int id, char      * data, size_t size);
template void fastContext::parallelize(unsigned long int id, int       * data, size_t size);
template void fastContext::parallelize(unsigned long int id, long int  * data, size_t size);
template void fastContext::parallelize(unsigned long int id, float     * data, size_t size);
template void fastContext::parallelize(unsigned long int id, double    * data, size_t size);


template <typename T>
void fastContext::parallelize(unsigned long int id, T ** data, size_t * dataSizes, size_t size){
	size_t offset = 0;

	for (int i = 1; i < comm->numProcs; ++i){
		int dataPerProc = size/ (comm->numProcs - 1);
		int rem = size % (comm->numProcs -1);
		if (i <= rem)
			dataPerProc += 1;
		std::cerr << "    S:FDDSetData P" << i << " " << id << " " << dataPerProc << "B";

		comm->sendFDDSetData(id, i, (void **) &data[offset], &dataSizes[offset], dataPerProc, sizeof(T));
		offset += dataPerProc;
		std::cerr << ".\n";
	}
}
template void fastContext::parallelize(unsigned long int id, char *    * data, size_t * dataSizes, size_t size);
template void fastContext::parallelize(unsigned long int id, int *     * data, size_t * dataSizes, size_t size);
template void fastContext::parallelize(unsigned long int id, long int ** data, size_t * dataSizes, size_t size);
template void fastContext::parallelize(unsigned long int id, float *   * data, size_t * dataSizes, size_t size);
template void fastContext::parallelize(unsigned long int id, double *  * data, size_t * dataSizes, size_t size);


template <typename T>
void fastContext::parallelize(unsigned long int id, std::vector<T> * data, size_t size){
	size_t offset = 0;

	for (int i = 1; i < comm->numProcs; ++i){
		int dataPerProc = size/ (comm->numProcs - 1);
		int rem = size % (comm->numProcs -1);
		if (i <= rem)
			dataPerProc += 1;
		std::cerr << "    S:FDDSetData P" << i << " " << id << " " << dataPerProc << "B";

		comm->sendFDDSetData(id, i, &data[offset], dataPerProc);
		offset += dataPerProc;
		std::cerr << ".\n";
	}
}
template void fastContext::parallelize(unsigned long int id, std::vector<char    > * data, size_t size);
template void fastContext::parallelize(unsigned long int id, std::vector<int     > * data, size_t size);
template void fastContext::parallelize(unsigned long int id, std::vector<long int> * data, size_t size);
template void fastContext::parallelize(unsigned long int id, std::vector<float   > * data, size_t size);
template void fastContext::parallelize(unsigned long int id, std::vector<double  > * data, size_t size);


void fastContext::parallelize(unsigned long int id, std::string * data, size_t size){
	size_t offset = 0;

	for (int i = 1; i < comm->numProcs; ++i){
		int dataPerProc = size/ (comm->numProcs - 1);
		int rem = size % (comm->numProcs -1);
		if (i <= rem)
			dataPerProc += 1;
		std::cerr << "    S:FDDSetData P" << i << " " << id << " " << dataPerProc << "B";

		comm->sendFDDSetData(id, i, &data[offset], dataPerProc);
		offset += dataPerProc;
		std::cerr << ".\n";
	}
}

void fastContext::destroyFDD(unsigned long int id){
	comm->sendDestroyFDD(id);
}


