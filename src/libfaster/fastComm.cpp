#include <algorithm>

#include "fastComm.h"
#include "workerIFdd.h"
#include "workerFdd.h"

bool fastComm::isDriver(){
	return (procId == 0);
}

fastComm::fastComm(const std::string UNUSED master){
	//MPI_Init (&argc, &argv);
	MPI_Init (0, NULL);
	MPI_Comm_size (MPI_COMM_WORLD, &numProcs);
	MPI_Comm_rank (MPI_COMM_WORLD, &procId);
	
	timeStart = MPI_Wtime();

	status = new MPI_Status [numProcs];
	req = new MPI_Request [numProcs];
	req2 = new MPI_Request [numProcs];
	buffer = new fastCommBuffer [numProcs];
	buffer2 = new fastCommBuffer [numProcs];
}

fastComm::~fastComm(){
	timeEnd = MPI_Wtime();
	MPI_Finalize (); 
	delete [] status;
	delete [] req;
	delete [] req2;
}

void fastComm::probeMsgs(int & tag){
	MPI_Status stat;
	MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &stat);
	tag = stat.MPI_TAG;
}

void fastComm::waitForReq(int numReqs){
	MPI_Waitall(numReqs, req, MPI_STATUSES_IGNORE);
}


void fastComm::sendTask(fastTask &task){
	buffer[0].reset();

	buffer[0] << task.id << task.operationType << task.srcFDD << task.destFDD << task.functionId;

	for (int i = 1; i < numProcs; ++i){
		MPI_Isend(buffer[0].data(), buffer[0].size(), MPI_BYTE, i, MSG_TASK, MPI_COMM_WORLD, &req[i-1]);
	}
	MPI_Waitall( numProcs - 1, req, status);
}

void fastComm::recvTask(fastTask & task){
	MPI_Status stat;

	buffer[0].reset();

	MPI_Recv(buffer[0].data(), buffer[0].free(), MPI_BYTE, 0, MSG_TASK, MPI_COMM_WORLD, &stat);	
	
	buffer[0] >> task.id >> task.operationType >> task.srcFDD >> task.destFDD >> task.functionId;
}

void fastComm::sendTaskResult(unsigned long int id, void * res, size_t size, double time){

	buffer[0].reset();

	buffer[0] << id << time << size;
	buffer[0].write(res, size);

	MPI_Isend(buffer[0].data(), buffer[0].size() , MPI_BYTE, 0, MSG_TASKRESULT, MPI_COMM_WORLD, req);
}

void * fastComm::recvTaskResult(unsigned long int & id, int & proc, size_t & size, double & time){

	buffer[0].reset();

	MPI_Recv(buffer[0].data(), buffer[0].free(), MPI_BYTE, MPI_ANY_SOURCE, MSG_TASKRESULT, MPI_COMM_WORLD, status);	
	buffer[0] >> id >> time >> size;
	proc = status->MPI_SOURCE;
	void * res = buffer[0].pos();
	//char * res = new char[size];
	//buffer[0].read(res, size);

	return res;
}

void fastComm::sendCreateFDD(unsigned long int id, fddType type, size_t size, int dest){
	//char typeC = encodeFDDType(type);

	buffer[dest].reset();

	//buffer[dest] << id << typeC << size;
	buffer[dest] << id << type << size;

	//std::cerr << '(' << id << ' ' << (int) typeC << ":" << buffer[dest].size() << ')';

	MPI_Isend(buffer[dest].data(), buffer[dest].size(), MPI_BYTE, dest, MSG_CREATEFDD, MPI_COMM_WORLD, &req[dest-1]);
}

void fastComm::recvCreateFDD(unsigned long int &id, fddType &type, size_t &size){
	//char t;

	buffer[0].reset();

	MPI_Recv(buffer[0].data(), buffer[0].free(), MPI_BYTE, 0, MSG_CREATEFDD, MPI_COMM_WORLD, status);	

	//buffer[0] >> id >> t >> size;
	//type = decodeFDDType(t);
	buffer[0] >> id >> type >> size;

	//std::cerr << '(' << id << ' ' << (int) t << ":" << buffer[dest].size()  << ')';
}

void fastComm::sendCreateIFDD(unsigned long int id, fddType kType,  fddType tType,  size_t size, int dest){
	//char typeC = encodeFDDType(type);

	buffer[dest].reset();

	//buffer[dest] << id << typeC << size;
	buffer[dest] << id << kType << tType << size;

	//std::cerr << '(' << id << ' ' << (int) typeC << ":" << buffer[dest].size() << ')';

	MPI_Isend(buffer[dest].data(), buffer[dest].size(), MPI_BYTE, dest, MSG_CREATEIFDD, MPI_COMM_WORLD, &req[dest-1]);
}

void fastComm::recvCreateIFDD(unsigned long int &id, fddType &kType, fddType &tType,  size_t &size){
	//char t;

	buffer[0].reset();

	MPI_Recv(buffer[0].data(), buffer[0].free(), MPI_BYTE, 0, MSG_CREATEIFDD, MPI_COMM_WORLD, status);	

	//buffer[0] >> id >> t >> size;
	//type = decodeFDDType(t);
	buffer[0] >> id >> kType >> tType >> size;

	//std::cerr << '(' << id << ' ' << (int) t << ":" << buffer[dest].size()  << ')';
}

void fastComm::sendDestroyFDD(unsigned long int id){
	for (int i = 1; i < numProcs; ++i){
		MPI_Isend( &id, sizeof(long unsigned int), MPI_BYTE, i, MSG_DESTROYFDD, MPI_COMM_WORLD, &req[i-1]);
	}

	//MPI_Waitall( numProcs - 1, req, status);
}
void fastComm::recvDestroyFDD(unsigned long int &id){
	MPI_Recv(&id, sizeof(long unsigned int), MPI_BYTE, 0, MSG_DESTROYFDD, MPI_COMM_WORLD, status);	
}


/* ------- DATA Serialization -------- */

// TODO use serialization?
// Generic Data communication functions
// Send 1D Data
void fastComm::sendDataGeneric(unsigned long int id, int dest, void * data, size_t size, int tagID, int tagData){
	buffer[dest].reset();

	buffer[dest] << id << size;

	MPI_Isend( buffer[dest].data(), buffer[dest].size(), MPI_BYTE, dest, tagID , MPI_COMM_WORLD, &req2[dest-1]);
	MPI_Isend( data, size, MPI_BYTE, dest, tagData, MPI_COMM_WORLD, &req[dest-1]);
}

// Send Container (String and Vector) Data
template <typename T>
void fastComm::sendDataGenericC(unsigned long int id, int dest, T * data, size_t size, int tagID, int tagData){
	buffer[dest].reset();

	// Send data information
	buffer[dest] << id << size;
	MPI_Isend( buffer[dest].data(), buffer[dest].size(), MPI_BYTE, dest, tagID , MPI_COMM_WORLD, &req2[dest-1]);

	buffer[dest].reset();
	// Send subarrays
	for ( size_t i = 0; i < size; ++i){
		buffer[dest] << data[i];
	}
	MPI_Isend( buffer[dest].data(), buffer[dest].size(),  MPI_BYTE, dest, tagData, MPI_COMM_WORLD, &req[dest-1]);
	//MPI_Send( buffer[dest].data(), buffer[dest].size(),  MPI_BYTE, dest, tagData, MPI_COMM_WORLD);
}

// Send 2D Data
void fastComm::sendDataGeneric(unsigned long int id, int dest, void ** data, size_t * lineSizes, size_t size, size_t itemSize, int tagID, int tagDataSize, int tagData){
	MPI_Request * localReq = new MPI_Request[size];
	MPI_Status * stat = new MPI_Status[size];

	// Send data information
	buffer[dest].reset();
	buffer[dest] << id << size << itemSize;
	MPI_Send( buffer[dest].data(), buffer[dest].size(), MPI_BYTE, dest, tagID , MPI_COMM_WORLD);

	// Send subarrays sizes
	MPI_Send( lineSizes, size*sizeof(size_t), MPI_BYTE, dest, tagDataSize, MPI_COMM_WORLD);

	//size_t ds;
	//ds = 0;
	//for ( size_t i = 0; i < size; ++i){
		//ds += lineSizes[i];
	//}
	//std::cerr << "Total data:"<< ds << "\n";

	// Send subarrays
	for ( size_t i = 0; i < size; ++i){
		MPI_Isend( data[i], lineSizes[i]*itemSize, MPI_BYTE, dest, tagData, MPI_COMM_WORLD, &localReq[i]);
	}
		
	MPI_Waitall( size, localReq, stat);

	delete localReq;
	delete stat;
}


// Receive 1D data
void fastComm::recvDataGeneric(unsigned long int &id, int src, void *& data, size_t &size, int tagID, int tagData){
	MPI_Status stat;
	int recvDataSize;

	buffer[src].reset();

	// Receive the FDD ID
	MPI_Recv(buffer[src].data(), buffer[src].free(), MPI_BYTE, src, tagID, MPI_COMM_WORLD, &stat);	
	buffer[src] >> id >> size;

	MPI_Probe(src, tagData, MPI_COMM_WORLD, &stat);
	MPI_Get_count(&stat, MPI_BYTE, &recvDataSize);

	buffer[src].reset();
	buffer[src].grow(recvDataSize);

	MPI_Recv(buffer[src].data(), buffer[src].free(), MPI_BYTE, src, tagData, MPI_COMM_WORLD, &stat);	

	for ( int i = 0; i < recvDataSize/4; ++i )
		std::cerr << ((int*) buffer[src].data())[i] << " ";
	
	data = buffer[src].data();
}


// Receive 2D data
void fastComm::recvDataGeneric(unsigned long int &id, int src, void **& data, size_t *& lineSizes, size_t &size, int tagID, int tagDataSize, int tagData){
	buffer[src].reset();
	size_t ds, itemSize;
	int recvDataSize;
	MPI_Status * stat;
	MPI_Request * localReq;

	// Receive the FDD ID, size and item size
	MPI_Recv(buffer[src].data(), buffer[src].free(), MPI_BYTE, src, tagID, MPI_COMM_WORLD, status);	
	buffer[src] >> id >> size >> itemSize;


	localReq = new MPI_Request[size];
	stat = new MPI_Status[size];

	MPI_Probe(src, tagDataSize, MPI_COMM_WORLD, status);
	MPI_Get_count(status, MPI_BYTE, &recvDataSize);
	buffer2[src].grow(recvDataSize);
	buffer2[src].reset();

	// Receive the size of every line to be received
	MPI_Recv(buffer2[src].data(), buffer2[src].free(), MPI_BYTE, src, tagDataSize, MPI_COMM_WORLD, status);	
	lineSizes = (size_t *) buffer2[src].data();

	ds = 0;
	// Figure out the size the busffer needs to be
	for ( size_t i = 0; i < size; ++i){
		ds += lineSizes[i];
	}

	buffer[src].grow(ds*itemSize);
	buffer[src].reset();

	data = new void*[size];
	// Receive a array of arrays
	for ( size_t i = 0; i < size; ++i){
		MPI_Irecv(buffer[src].pos(), buffer[src].free(), MPI_BYTE, src, tagData, MPI_COMM_WORLD, &localReq[i]);	
		data[i] = buffer[src].pos();
		buffer[src].advance(lineSizes[i]*itemSize);
	}
	MPI_Waitall( size, localReq, stat);
	
	delete localReq;
}


// 1D Primitive types
void fastComm::sendFDDSetData(unsigned long int id, int dest, void * data, size_t size){
	sendDataGeneric(id, dest, data, size, MSG_FDDSETDATAID, MSG_FDDSETDATA);
}

void fastComm::recvFDDSetData(unsigned long int &id, void *& data, size_t &size){
	recvDataGeneric(id, 0, data, size, MSG_FDDSETDATAID, MSG_FDDSETDATA);
}

// 2D Primitive types
void fastComm::sendFDDSetData(unsigned long int id, int dest, void ** data, size_t *lineSize, size_t size, size_t itemSize){
	sendDataGeneric(id, dest, data, lineSize, size, itemSize, MSG_FDDSET2DDATAID, MSG_FDDSET2DDATASIZES, MSG_FDDSET2DDATA);
}

void fastComm::recvFDDSetData(unsigned long int &id, void **& data, size_t *& lineSizes, size_t &size){
	recvDataGeneric(id, 0, data, lineSizes, size, MSG_FDDSET2DDATAID, MSG_FDDSET2DDATASIZES, MSG_FDDSET2DDATA);
}

// Containers
void fastComm::sendFDDSetData(unsigned long int id, int dest, std::string * data, size_t size){
	sendDataGenericC(id, dest, data, size, MSG_FDDSETDATAID, MSG_FDDSETDATA);
}

template <typename T>
void fastComm::sendFDDSetData(unsigned long int id, int dest, std::vector<T> * data, size_t size){
	sendDataGenericC(id, dest, data, size, MSG_FDDSETDATAID, MSG_FDDSETDATA);
}

template void fastComm::sendFDDSetData(unsigned long int id, int dest, std::vector<char    > * data, size_t size);
template void fastComm::sendFDDSetData(unsigned long int id, int dest, std::vector<int     > * data, size_t size);
template void fastComm::sendFDDSetData(unsigned long int id, int dest, std::vector<long int> * data, size_t size);
template void fastComm::sendFDDSetData(unsigned long int id, int dest, std::vector<float   > * data, size_t size);
template void fastComm::sendFDDSetData(unsigned long int id, int dest, std::vector<double  > * data, size_t size);





// Parallel data communication
void fastComm::sendFDDData(unsigned long int id, int dest, void * data, size_t size){
	sendDataGeneric(id, dest, data, size, MSG_FDDDATAID, MSG_FDDDATA);
}

void fastComm::recvFDDData(unsigned long int &id, void * data, size_t &size){
	recvDataGeneric(id, 0, data, size, MSG_FDDDATAID, MSG_FDDDATA);
}




void fastComm::sendReadFDDFile(unsigned long int id, std::string filename, size_t size, size_t offset, int dest){
	
	buffer[dest].reset();

	buffer[dest] << id << size << offset << filename;

	MPI_Isend( buffer[dest].data(), buffer[dest].size(), MPI_BYTE, dest, MSG_READFDDFILE, MPI_COMM_WORLD, &req[dest]);
}

void fastComm::recvReadFDDFile(unsigned long int &id, std::string & filename, size_t &size, size_t & offset){
	size_t filenameSize;

	buffer[0].reset();

	MPI_Recv(buffer[0].data(), buffer[0].free(), MPI_BYTE, 0, MSG_READFDDFILE, MPI_COMM_WORLD, status);	
	buffer[0] >> id >> size >> offset >> filenameSize;
	buffer[0].read(filename, filenameSize);
}


void fastComm::sendFDDInfo(size_t size){
	MPI_Send( &size, sizeof(size_t), MPI_BYTE, 0, MSG_FDDINFO, MPI_COMM_WORLD);
}

void fastComm::recvFDDInfo(size_t &size){
	MPI_Recv(&size, sizeof(size_t), MPI_BYTE, MPI_ANY_SOURCE, MSG_FDDINFO, MPI_COMM_WORLD, status);
}


void fastComm::sendCollect(unsigned long int id){
	for (int i = 1; i < numProcs; ++i){
		MPI_Isend( &id, sizeof(long unsigned int), MPI_BYTE, i, MSG_COLLECT, MPI_COMM_WORLD, &req[i-1]);
	}

	//MPI_Waitall( numProcs - 1, req, status);
}

void fastComm::recvCollect(unsigned long int &id){
	MPI_Recv(&id, sizeof(long unsigned int), MPI_BYTE, 0, MSG_COLLECT, MPI_COMM_WORLD, status);	
}

void fastComm::sendFinish(){
	for (int i = 1; i < numProcs; ++i){
		MPI_Isend( buffer[0].data(), 1, MPI_BYTE, i, MSG_FINISH, MPI_COMM_WORLD, &req[i-1]);
	}

	MPI_Waitall( numProcs - 1, req, status);
}
void fastComm::recvFinish(){
	MPI_Recv(buffer[0].data(), 1, MPI_BYTE, 0, MSG_FINISH, MPI_COMM_WORLD, status);	
}

