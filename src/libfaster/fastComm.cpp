#include <algorithm>

#include "fastComm.h"

char encodeFDDType(fddType type){
	switch (type){
		case Int:
			return FDDTYPE_INT;
		case LongInt:
			return FDDTYPE_LONGINT;
		case Float:
			return FDDTYPE_FLOAT;
		case Double:
			return FDDTYPE_DOUBLE;
		case String:
			return FDDTYPE_STRING;
		case Custom:
			return FDDTYPE_OBJECT;
		case Null:
			return FDDTYPE_NULL;
	}
}
fddType decodeFDDType(char type){
	switch (type){
		case FDDTYPE_INT:
			return Int;
		case FDDTYPE_LONGINT:
			return LongInt;
		case FDDTYPE_FLOAT:
			return Float;
		case FDDTYPE_DOUBLE:
			return Double;
		case FDDTYPE_STRING:
			return String;
		case FDDTYPE_OBJECT:
			return Custom;
		case FDDTYPE_NULL:
			return Null;
	}
}

template <> void fastCommBuffer::write<std::string>(std::string s){
	write( s.length() );
	write( s.c_str(), s.length()+1 );
}

template <> void fastCommBuffer::read<std::string>(std::string & s){
	size_t sSize;
	read(sSize);
	std::string tmpStr(_data[_size], sSize);
}


bool fastComm::isDriver(){
	return (procId == 0);
}

fastComm::fastComm(const std::string master){
	//MPI_Init (&argc, &argv);
	MPI_Init (NULL, NULL);
	MPI_Comm_size (MPI_COMM_WORLD, &numProcs);
	MPI_Comm_rank (MPI_COMM_WORLD, &procId);
	
	timeStart = MPI_Wtime();

	status = new MPI_Status [numProcs];
	req = new MPI_Request [numProcs];
	req2 = new MPI_Request [numProcs];
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
	MPI_Waitall(numReqs, &req[1], MPI_STATUSES_IGNORE);
}


void fastComm::sendTask(fastTask * &t){
	for (int i = 1; i < numProcs; ++i){
		MPI_Isend(t, sizeof(fastTask) , MPI_BYTE, i, MSG_TASK, MPI_COMM_WORLD, &req[i-1]);
	}
	MPI_Waitall( numProcs - 1, req, status);
}

void fastComm::recvTask(fastTask & task){
	MPI_Status stat;
	MPI_Recv(&task, sizeof(fastTask), MPI_BYTE, 0, MSG_TASK, MPI_COMM_WORLD, &stat);	
}

void fastComm::sendTaskResult(unsigned long int id, void * res, size_t size, double time){

	buffer.reset();
	buffer << id << time << size;
	buffer.write(res);

	MPI_Send(buffer.data(), buffer.size() , MPI_BYTE, 0, MSG_TASKRESULT, MPI_COMM_WORLD);
}

void fastComm::recvTaskResult(unsigned long int id, void * res, size_t & size, double & time){

	buffer.reset();

	MPI_Recv(buffer.data(), buffer.free(), MPI_BYTE, MPI_ANY_SOURCE, MSG_TASKRESULT, MPI_COMM_WORLD, status);	

	buffer >> id >> time >> size;
	buffer.read(res, size);
}

void fastComm::sendCreateFDD(unsigned long int id, fddType type, size_t size){
	char typeC = encodeFDDType(type);

	buffer.reset();

	buffer << id << typeC << size;

	//std::cerr << '(' << id << ' ' << (int) typeC << ":" << buffer.size() << ')';

	for (int i = 1; i < numProcs; ++i){
		MPI_Isend(buffer.data(), buffer.size(), MPI_BYTE, i, MSG_CREATEFDD, MPI_COMM_WORLD, &req[i-1]);
	}

	MPI_Waitall( numProcs - 1, req, status);
}

void fastComm::recvCreateFDD(unsigned long int &id, fddType &type, size_t &size){
	char t;

	buffer.reset();

	MPI_Recv(buffer.data(), buffer.free(), MPI_BYTE, 0, MSG_CREATEFDD, MPI_COMM_WORLD, status);	

	buffer >> id >> t >> size;
	type = decodeFDDType(t);

	//std::cerr << '(' << id << ' ' << (int) t << ":" << buffer.size()  << ')';
}

void fastComm::sendDestroyFDD(unsigned long int id){
	for (int i = 1; i < numProcs; ++i){
		MPI_Isend( &id, sizeof(long unsigned int), MPI_BYTE, i, MSG_DESTROYFDD, MPI_COMM_WORLD, &req[i-1]);
	}

	MPI_Waitall( numProcs - 1, req, status);
}
void fastComm::recvDestroyFDD(unsigned long int &id){
	MPI_Recv(&id, sizeof(long unsigned int), MPI_BYTE, 0, MSG_DESTROYFDD, MPI_COMM_WORLD, status);	
}


/* ------- DATA Serialization -------- */

// TODO use serialization?
// Parallelization data communication
void fastComm::sendFDDSetData(unsigned long int id, int dest, void * data, size_t size){
	buffer.reset();

	buffer << id << size;

	MPI_Isend( buffer.data(), buffer.size(), MPI_BYTE, dest, MSG_FDDSETDATAID, MPI_COMM_WORLD, &req2[dest]);
	MPI_Isend( data, size, MPI_BYTE, dest, MSG_FDDSETDATA, MPI_COMM_WORLD, &req[dest]);
}

void fastComm::recvFDDSetData(unsigned long int &id, void *& data, size_t &size){

	buffer.reset();

	// Receive the FDD ID
	MPI_Recv(buffer.data(), buffer.free(), MPI_BYTE, 0, MSG_FDDSETDATAID, MPI_COMM_WORLD, status);	
	buffer >> id >> size;

	// Receive the FDD DATA
	//MPI_Probe(MPI_ANY_SOURCE, MSG_FDDSETDATA, MPI_COMM_WORLD, status);
	//MPI_Get_count(status, MPI_CHAR, &s);

	buffer.grow(size);
	buffer.reset();

	MPI_Recv(buffer.data(), buffer.free(), MPI_BYTE, 0, MSG_FDDSETDATA, MPI_COMM_WORLD, status);	
	data = buffer.data();
}

// Generic Data communication functions
void fastComm::sendFDDData(unsigned long int id, int dest, void * data, size_t size){
	buffer.reset();

	buffer << id << size;

	MPI_Isend( buffer.data(), buffer.size(), MPI_BYTE, dest, MSG_FDDDATAID, MPI_COMM_WORLD, &req2[dest]);
	MPI_Isend( data, size, MPI_BYTE, dest, MSG_FDDDATA, MPI_COMM_WORLD, &req[dest]);
}

void fastComm::recvFDDData(unsigned long int &id, void *& data, size_t &size){

	buffer.reset();

	// Receive the FDD ID
	MPI_Recv(buffer.data(), buffer.free(), MPI_BYTE, MPI_ANY_SOURCE, MSG_FDDDATAID, MPI_COMM_WORLD, status);	

	buffer >> id >> size;

	// Receive the FDD DATA
	//MPI_Probe(MPI_ANY_SOURCE, MSG_FDDSETDATA, MPI_COMM_WORLD, status);
	//MPI_Get_count(status, MPI_CHAR, &s);

	buffer.grow(size);
	buffer.reset();

	MPI_Recv(buffer.data(), buffer.free(), MPI_BYTE, MPI_ANY_SOURCE, MSG_FDDDATA, MPI_COMM_WORLD, status);	
	data = buffer.data();
}






void fastComm::sendReadFDDFile(unsigned long int id, std::string filename, size_t size, size_t offset, int dest){
	
	buffer.reset();

	buffer << id << size << offset << filename;

	MPI_Isend( buffer.data(), buffer.size(), MPI_BYTE, dest, MSG_READFDDFILE, MPI_COMM_WORLD, &req[dest]);
}

void fastComm::recvReadFDDFile(unsigned long int &id, std::string & filename, size_t &size, size_t & offset){
	buffer.reset();

	MPI_Recv(buffer.data(), buffer.free(), MPI_BYTE, 0, MSG_READFDDFILE, MPI_COMM_WORLD, status);	
	buffer >> id >> size >> offset >> filename;
}


void fastComm::sendFDDInfo(size_t size){
	MPI_Send( &size, sizeof(size_t), MPI_BYTE, 0, MSG_FDDINFO, MPI_COMM_WORLD);
}

void fastComm::recvFDDInfo(size_t &size){
	MPI_Recv(&size, sizeof(long unsigned int), MPI_BYTE, MPI_ANY_SOURCE, MSG_FDDINFO, MPI_COMM_WORLD, status);
}


void fastComm::sendCollect(unsigned long int id){
	for (int i = 1; i < numProcs; ++i){
		MPI_Isend( &id, sizeof(long unsigned int), MPI_BYTE, i, MSG_COLLECT, MPI_COMM_WORLD, &req[i-1]);
	}

	MPI_Waitall( numProcs - 1, req, status);
}

void fastComm::recvCollect(unsigned long int &id){
	MPI_Recv(&id, sizeof(long unsigned int), MPI_BYTE, 0, MSG_COLLECT, MPI_COMM_WORLD, status);	
}

void fastComm::sendFinish(){
	for (int i = 1; i < numProcs; ++i){
		MPI_Isend( buffer.data(), 1, MPI_BYTE, i, MSG_FINISH, MPI_COMM_WORLD, &req[i-1]);
	}

	MPI_Waitall( numProcs - 1, req, status);
}
void fastComm::recvFinish(){
	MPI_Recv(buffer.data(), 1, MPI_BYTE, 0, MSG_FINISH, MPI_COMM_WORLD, status);	
}

