#include <algorithm>

#include "fastComm.h"

char encodeFDDType(fddType type){
	switch (type){
		case Char:
			return FDDTYPE_CHAR;
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
		case CharP:
			return FDDTYPE_CHARP;
		case IntP:
			return FDDTYPE_INTP;
		case LongIntP:
			return FDDTYPE_LONGINTP;
		case FloatP:
			return FDDTYPE_FLOATP;
		case DoubleP:
			return FDDTYPE_DOUBLEP;
		case Custom:
			return FDDTYPE_OBJECT;
		case Null:
			return FDDTYPE_NULL;
	}
}
fddType decodeFDDType(char type){
	switch (type){
		case FDDTYPE_CHAR:
			return Char;
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
		case FDDTYPE_CHARP:
			return CharP;
		case FDDTYPE_INTP:
			return IntP;
		case FDDTYPE_LONGINTP:
			return LongIntP;
		case FDDTYPE_FLOATP:
			return FloatP;
		case FDDTYPE_DOUBLEP:
			return DoubleP;
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
	MPI_Init (0, NULL);
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


void fastComm::sendTask(fastTask &task){
	buffer.reset();

	buffer << task.id << task.operationType << task.destFDD << task.srcFDD << task.functionId;

	for (int i = 1; i < numProcs; ++i){
		MPI_Isend(buffer.data(), buffer.size(), MPI_BYTE, i, MSG_TASK, MPI_COMM_WORLD, &req[i-1]);
	}
	MPI_Waitall( numProcs - 1, req, status);
}

void fastComm::recvTask(fastTask & task){
	MPI_Status stat;

	buffer.reset();

	MPI_Recv(buffer.data(), buffer.free(), MPI_BYTE, 0, MSG_TASK, MPI_COMM_WORLD, &stat);	
	
	buffer >> task.id >> task.operationType >> task.destFDD >> task.srcFDD >> task.functionId;
}

void fastComm::sendTaskResult(unsigned long int id, void * res, size_t size, double time){

	buffer.reset();

	buffer << id << time << size;
	buffer.write(res, size);

	MPI_Send(buffer.data(), buffer.size() , MPI_BYTE, 0, MSG_TASKRESULT, MPI_COMM_WORLD);
}

void fastComm::recvTaskResult(unsigned long int &id, void * res, size_t & size, double & time){
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
void fastComm::sendDataGeneric(unsigned long int id, int dest, void * data, size_t size, int tagID, int tagData){
	buffer.reset();

	buffer << id << size;

	MPI_Isend( buffer.data(), buffer.size(), MPI_BYTE, dest, tagID , MPI_COMM_WORLD, &req2[dest]);
	MPI_Isend( data, size, MPI_BYTE, dest, tagData, MPI_COMM_WORLD, &req[dest]);
}

void fastComm::recvDataGeneric(unsigned long int &id, void *& data, size_t &size, int tagID, int tagData){
	buffer.reset();

	// Receive the FDD ID
	MPI_Recv(buffer.data(), buffer.free(), MPI_BYTE, 0, MSG_FDDSETDATAID, MPI_COMM_WORLD, status);	
	buffer >> id >> size;

	buffer.grow(size);
	buffer.reset();

	MPI_Recv(buffer.data(), buffer.free(), MPI_BYTE, 0, MSG_FDDSETDATA, MPI_COMM_WORLD, status);	
	data = buffer.data();
}

void fastComm::sendDataGeneric(unsigned long int id, int dest, void ** data, size_t * lineSize, size_t size, size_t itemSize, int tagID, int tagDataSize, int tagData){
	MPI_Request * localReq = new MPI_Request[size];
	MPI_Status stat;

	buffer.reset();

	// Send data information
	buffer << id << size << itemSize;
	MPI_Send( buffer.data(), buffer.size(), MPI_BYTE, dest, tagID , MPI_COMM_WORLD);

	// Send subarrays sizes
	MPI_Send( lineSize, size*sizeof(size_t), MPI_BYTE, dest, tagDataSize, MPI_COMM_WORLD);
	for ( size_t i = 0; i < size; ++i){
		MPI_Isend( data[i], lineSize[i]*itemSize, MPI_BYTE, dest, tagData, MPI_COMM_WORLD, &localReq[i]);
	}
		
	MPI_Waitall( size, localReq, &stat);

	delete localReq;
}

void fastComm::recvDataGeneric(unsigned long int &id, void **& data, size_t * lineSize, size_t &size, int tagID, int tagDataSize, int tagData){
	buffer.reset();
	size_t ds, itemSize;
	MPI_Status stat;

	// Receive the FDD ID, size and item size
	MPI_Recv(buffer.data(), buffer.free(), MPI_BYTE, 0, MSG_FDDSETDATAID, MPI_COMM_WORLD, status);	
	buffer >> id >> size >> itemSize;

	MPI_Request * localReq = new MPI_Request[size];

	buffer2.grow(size*itemSize);
	buffer2.reset();

	// Receive the size of every line to be received
	MPI_Recv(buffer2.data(), buffer2.free(), MPI_BYTE, 0, MSG_FDDSETDATA, MPI_COMM_WORLD, status);	
	lineSize = (size_t *) buffer2.data();

	// Figure out the size the busffer needs to be
	for ( size_t i = 0; i < size; ++i){
		ds += lineSize[i]*itemSize;
	}

	buffer.grow(ds);
	buffer.reset();

	// Receive a array of arrays
	for ( size_t i = 0; i < size; ++i){
		MPI_Irecv(buffer.data(), buffer.free(), MPI_BYTE, 0, MSG_FDDSETDATA, MPI_COMM_WORLD, &localReq[i]);	
		data[i] = buffer.pos();
		buffer.advance(lineSize[i]*itemSize);
	}
	MPI_Waitall( size, localReq, &stat);
}

void fastComm::sendFDDSetData(unsigned long int id, int dest, void * data, size_t size){
	sendDataGeneric(id, dest, data, size, MSG_FDDSETDATAID, MSG_FDDSETDATA);
}

void fastComm::recvFDDSetData(unsigned long int &id, void *& data, size_t &size){
	recvDataGeneric(id, data, size, MSG_FDDSETDATAID, MSG_FDDSETDATA);
}

void fastComm::sendFDDSetData(unsigned long int id, int dest, void ** data, size_t *lineSize, size_t size, size_t itemSize){
	sendDataGeneric(id, dest, data, lineSize, size, itemSize, MSG_FDDSET2DDATAID, MSG_FDDSET2DDATASIZES, MSG_FDDSET2DDATA);
}

void fastComm::recvFDDSetData(unsigned long int &id, void **& data, size_t *lineSize, size_t &size){
	recvDataGeneric(id, data, lineSize, size, MSG_FDDSET2DDATAID, MSG_FDDSET2DDATASIZES, MSG_FDDSET2DDATA);
}

// Generic Data communication functions
void fastComm::sendFDDData(unsigned long int id, int dest, void * data, size_t size){
	sendDataGeneric(id, dest, data, size, MSG_FDDDATAID, MSG_FDDDATA);
}

void fastComm::recvFDDData(unsigned long int &id, void *& data, size_t &size){
	recvDataGeneric(id, data, size, MSG_FDDDATAID, MSG_FDDDATA);
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

