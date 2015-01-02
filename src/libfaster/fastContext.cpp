#include <chrono>
#include <fstream>

#include "worker.h"
#include "fastContext.h"
#include "misc.h"

// Create a context with local as master
faster::fastContext::fastContext(const fastSettings & s, int & argc, char **& argv){

	settings = new fastSettings(s);
	comm = new fastComm(argc, argv );
	scheduler = new fastScheduler( comm->numProcs, & funcName);
	numFDDs = 0;
	//numTasks = 0;

}


faster::fastContext::~fastContext(){ 
	// Tell workers to go home!
	//std::cerr << "    S:FINISH! ";
	comm->sendFinish();

	// Clean process
	fddList.clear();
	//taskList.clear();
	delete comm;
	delete settings;
}

void faster::fastContext::registerFunction(void * funcP){
	funcTable.insert(funcTable.end(), funcP);
	funcName.insert(funcName.end(), "");
}

void faster::fastContext::registerFunction(void * funcP, std::string name){
	funcTable.insert(funcTable.end(), funcP);
	funcName.insert(funcName.end(), name);
}

void faster::fastContext::startWorkers(){
	// Create a Worker context and exit after finished
	if ( ! comm->isDriver() ){
		// Start worker role
		worker worker(comm, funcTable.data(), globalTable);

		worker.run();

		// Clean process
		delete comm; 
		delete settings; 
		exit(0);
	}// */

}

void faster::fastContext::calibrate(){
	using std::chrono::system_clock;
	using std::chrono::duration_cast;
	using std::chrono::milliseconds;

	unsigned long int rid;
	unsigned long int sid = 0;
	size_t size;
	std::vector<size_t> time(comm->numProcs, 0);

	auto start = system_clock::now();
	unsigned long int tid = enqueueTask(OP_Calibrate, 0, 1000);

	for (int i = 1; i < comm->numProcs; ++i){
		size_t t = 0;
		procstat stat;
		void * result UNUSED = comm->recvTaskResult(rid, sid, size, t, stat);
		time[sid] = t;
		scheduler->taskProgress(tid, sid, t, stat);
	}
	comm->waitForReq(comm->numProcs - 1);

	auto duration = duration_cast<milliseconds>(system_clock::now() - start);
	scheduler->taskFinished(tid, duration.count());

	scheduler->setCalibration(time);
}

int faster::fastContext::findFunc(void * funcP){
	//std::cerr << "  Find Function " << funcP ;
	for( size_t i = 0; i < funcTable.size(); ++i){
		if (funcTable[i] == funcP)
			return i;
	}
	return -1;
}


unsigned long int faster::fastContext::_createFDD(fddBase * ref, fddType type, const std::vector<size_t> * dataAlloc){
	
	//std::cerr << "    Create FDD\n";
	for (int i = 1; i < comm->numProcs; ++i){
		if (dataAlloc){
			//std::cerr << "    S:CreateFdd ID:" << numFDDs << " T:" << type << " S:" << (*dataAlloc)[i] ;
			comm->sendCreateFDD(numFDDs, type, (*dataAlloc)[i], i);
			//std::cerr << ".\n";
		}else{
			//std::cerr << "    S:CreateFdd ID:" << numFDDs << " T:" << type << " S: ?";
			comm->sendCreateFDD(numFDDs, type, 0, i);
			//std::cerr << ".\n";
		}

	}
	fddList.insert(fddList.begin(), ref);
	comm->waitForReq(comm->numProcs - 1);
	//std::cerr << "    Done\n";
	
	return numFDDs++;
}

unsigned long int faster::fastContext::_createIFDD(fddBase * ref, fddType kType, fddType tType, const std::vector<size_t> * dataAlloc){

	//std::cerr << "    Create IFDD\n";
	for (int i = 1; i < comm->numProcs; ++i){
		//std::cerr << "    S:CreateIFdd ID:" << numFDDs << " K:" << kType << " T:" << tType << " S:" << dataPerProc <<'\n';
		if (dataAlloc){
			//std::cerr << "    S:CreateIFdd ID:" << numFDDs << " K:" << kType << " T:" << tType << " S:" << (*dataAlloc)[i];
			comm->sendCreateIFDD(numFDDs, kType, tType, (*dataAlloc)[i], i);
			//std::cerr << ".\n";
		}else{
			//std::cerr << "    S:CreateIFdd ID:" << numFDDs << " K:" << kType << " T:" << tType << " S:? ";
			comm->sendCreateIFDD(numFDDs, kType, tType, 0, i);
			//std::cerr << ".\n";
		}
	}
	fddList.insert(fddList.begin(), ref);
	comm->waitForReq(comm->numProcs - 1);
	//std::cerr << "    Done\n";

	return numFDDs++;
}

// TODO CHANGE THIS!
unsigned long int faster::fastContext::createFDD(fddBase * ref, size_t typeCode, const std::vector<size_t> & dataAlloc){
	return _createFDD(ref, decodeType(typeCode), &dataAlloc);
}
unsigned long int faster::fastContext::createFDD(fddBase * ref, size_t typeCode){
	return _createFDD(ref, decodeType(typeCode), NULL);
}
unsigned long int faster::fastContext::createPFDD(fddBase * ref, size_t typeCode, const std::vector<size_t> & dataAlloc){
	return _createFDD(ref, POINTER | decodeType(typeCode), &dataAlloc);
}
unsigned long int faster::fastContext::createPFDD(fddBase * ref, size_t typeCode){
	return _createFDD(ref, POINTER | decodeType(typeCode), NULL);
}
unsigned long int faster::fastContext::createIFDD(fddBase * ref, size_t kTypeCode, size_t tTypeCode, const std::vector<size_t> & dataAlloc){
	return _createIFDD(ref, decodeType(kTypeCode), decodeType(tTypeCode), &dataAlloc);
}
unsigned long int faster::fastContext::createIFDD(fddBase * ref, size_t kTypeCode, size_t tTypeCode){
	return _createIFDD(ref, decodeType(kTypeCode), decodeType(tTypeCode), NULL);
}
unsigned long int faster::fastContext::createIPFDD(fddBase * ref, size_t kTypeCode, size_t tTypeCode, const std::vector<size_t> & dataAlloc){
	return _createIFDD(ref, decodeType(kTypeCode), POINTER | decodeType(tTypeCode), &dataAlloc);
}
unsigned long int faster::fastContext::createIPFDD(fddBase * ref, size_t kTypeCode, size_t tTypeCode){
	return _createIFDD(ref, decodeType(kTypeCode), POINTER | decodeType(tTypeCode), NULL);
}

unsigned long int faster::fastContext::createFddGroup(fddBase * ref, std::vector<fddBase*> & fddV){
	std::vector<unsigned long int> members(fddV.size());
	std::vector<fddType> dataTypeV(fddV.size());

	fddType kType = fddV[0]->kType();

	for( size_t i = 0; i < fddV.size(); ++i){
		members[i] = fddV[i]->getId();
	}

	comm->sendCreateFDDGroup( numFDDs, kType,  members);

	//std::cerr << "    S:CreateFddGroup ID:" << numFDDs << '\n';

	fddList.insert(fddList.begin(), ref);
	return numFDDs++;
}



// Propagate FDD destruction to other machines
size_t findFileSize(const char* filename)
{
	std::ifstream in(filename, std::ifstream::in | std::ifstream::binary);
	in.seekg(0, std::ifstream::end);
	return in.tellg(); 
}

unsigned long int faster::fastContext::readFDD(fddBase * ref, const char * fileName){
	//send read fdd n. numFdds from file fileName
	size_t fileSize = findFileSize(fileName);
	size_t offset = 0;
 	std::vector<size_t> dataAlloc = getAllocation(fileSize);

	for (int i = 1; i < comm->numProcs; ++i){
		//size_t dataPerProc = fileSize / (comm->numProcs - 1);
		//int rem = fileSize % (comm->numProcs -1);
		//if (i <= rem)
			//dataPerProc += 1;
		//comm->sendReadFDDFile(ref->getId(), std::string(fileName), dataPerProc, offset, i);
		//offset += dataPerProc;
		comm->sendReadFDDFile(ref->getId(), std::string(fileName), dataAlloc[i], offset, i);
		offset += dataAlloc[i];
	}

	//std::cerr << "    S:ReadFdd";
	comm->waitForReq(comm->numProcs - 1);
	//std::cerr << '\n';
	
	return numFDDs++;
}

void faster::fastContext::getFDDInfo(size_t & s, std::vector<size_t> & dataAlloc){
	s = 0;
	dataAlloc = std::vector<size_t>(comm->numProcs,0);

	for (int i = 1; i < comm->numProcs; ++i){
		size_t size;
		int src;

		//std::cerr << "    R:GetFDDInfo ";
		comm->recvFDDInfo(size, src);
		dataAlloc[src] = size;

		//std::cerr << "S:" << size << "\n";
		s += size;
	}
}


unsigned long int faster::fastContext::enqueueTask(fddOpType opT, unsigned long int idSrc, unsigned long int idRes, int funcId, size_t size){
	fastTask * newTask = scheduler->enqueueTask(opT, idSrc, idRes, funcId, size, globalTable);

	//if(scheduler->dataMigrationNeeded()){
		//comm->migrateData(scheduler->getDataMigrationInfo());
	//	scheduler->getDataMigrationInfo();
	//}

	// TODO do this later on a shceduler?
	comm->sendTask(*newTask);
	//std::cerr << "    S:Task ID:" << newTask->id << " FDD:" << idSrc << " F:" << funcId << '\n';

	return newTask->id;
}
unsigned long int faster::fastContext::enqueueTask(fddOpType opT, unsigned long int id, size_t size){
	return enqueueTask(opT, id, 0, -1, size);
}

std::vector< std::pair<void *, size_t> > faster::fastContext::recvTaskResult(unsigned long int &tid, unsigned long int &sid, system_clock::time_point & start){
	using std::chrono::duration_cast;
	using std::chrono::milliseconds;

	std::vector< std::pair<void*, size_t> > result ( comm->numProcs );
	//std::cerr << "    R:TaskResult \n";

	start =	system_clock::now();
	for ( int  i = 1; i < comm->numProcs; i++ ){
		size_t time;
		size_t size;
		procstat stat;

		void * r = comm->recvTaskResult(tid, sid, size, time, stat);
		result[sid].first = r;
		result[sid].second = size;
		//std::cerr << "        P: " << sid << "\n";

		scheduler->taskProgress(tid, sid, time, stat);
	}
	comm->waitForReq(comm->numProcs - 1);

	auto duration = duration_cast<milliseconds>(system_clock::now() - start);
	scheduler->taskFinished(tid, duration.count());


	//taskList[id]->workersFinished++;

	return result;
}

std::vector<size_t> faster::fastContext::getAllocation(size_t size){
	return scheduler->getAllocation(size);
}

void faster::fastContext::discardFDD(unsigned long int id){
	comm->sendDiscardFDD(id);
}

void faster::fastContext::updateInfo(){
	scheduler->updateTaskInfo();
}
void faster::fastContext::printHeader(){
	scheduler->printHeader();
}
void faster::fastContext::printInfo(){
	//comm->getHostnames();
	scheduler->printTaskInfo();
}


