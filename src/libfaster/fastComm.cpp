#include "fastComm.h"

char encodeFDDType(fddType & type){
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
		case Object:
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
			return Object;
		case FDDTYPE_NULL:
			return Null;
	}
}

bool fastComm::isDriver(){
	return procId ? true : false;
}

fastComm::fastComm(const std::string master){
	//MPI_Init (&argc, &argv);
	MPI_Init (NULL, NULL);
	MPI_Comm_size (MPI_COMM_WORLD, &numProcs);
	MPI_Comm_rank (MPI_COMM_WORLD, &procId);
	
	timeStart = MPI_Wtime();

	status = new MPI_Status [numProcs];
	req = new MPI_Request [numProcs];
	bufferSize = BUFFER_INITIAL_SIZE;
	buffer = new char [bufferSize];
}

fastComm::~fastComm(){
	timeEnd = MPI_Wtime();
	MPI_Finalize (); 
	delete [] status;
	delete [] req;
	delete [] buffer;
}

void fastComm::probeMsgs(int & tag){
	MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, status);
	tag = status->MPI_TAG;
}


void fastComm::sendTask(fastTask * &t){
	for (int i = 1; i < numProcs; ++i){
		MPI_Isend(t, sizeof(fastTask) , MPI_BYTE, i, MSG_TASK, MPI_COMM_WORLD, &req[i-1]);
	}
	MPI_Waitall( numProcs - 1, req, status);
}

void fastComm::recvTask(fastTask & task){
	MPI_Recv(&task, sizeof(fastTask), MPI_BYTE, 0, MSG_TASK, MPI_COMM_WORLD, status);	
}

void fastComm::sendTaskResult(unsigned long int id, void * res, double time){
}
void fastComm::recvTaskResult(unsigned long int id, void * res, double & time){
}

void fastComm::sendCreateFDD(unsigned long int id, fddType type){
	std::stringstream sbuffer;

	sbuffer << id << encodeFDDType(type);
	sbuffer.get(buffer, bufferSize);
	sbuffer.seekp(0, std::ios::end);

	for (int i = 1; i < numProcs; ++i){
		MPI_Isend(buffer, sbuffer.tellp() , MPI_BYTE, i, MSG_CREATEFDD, MPI_COMM_WORLD, &req[i-1]);
	}

	MPI_Waitall( numProcs - 1, req, status);
}
void fastComm::recvCreateFDD(unsigned long int &id, fddType &type){
	char t;

	MPI_Recv(buffer, sizeof(long unsigned int) + 1, MPI_BYTE, 0, MSG_CREATEFDD, MPI_COMM_WORLD, status);	
	std::stringstream decode(buffer);
	decode >> id >> t;
	type = decodeFDDType(t);
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

// TODO use serialization?
void fastComm::sendFDDData(unsigned long int id, int dest, void * data, size_t size){
	MPI_Isend( &id, sizeof(unsigned long int), MPI_BYTE, dest, MSG_FDDDATAID, MPI_COMM_WORLD, &req[dest]);
	MPI_Isend( data, size, MPI_BYTE, dest, MSG_FDDDATA, MPI_COMM_WORLD, &req[dest]);
}

void fastComm::waitForReq(int numReqs){
	MPI_Waitall(numReqs, &req[1], status);
}

void fastComm::recvFDDData(unsigned long int &id, void * data, size_t &size){
	int s = 1024;
	// Receive the FDD ID
	MPI_Recv(&id, s, MPI_BYTE, 0, MSG_FDDDATAID, MPI_COMM_WORLD, status);	

	// Receive the FDD DATA
	MPI_Probe(MPI_ANY_SOURCE, MSG_FDDDATA, MPI_COMM_WORLD, status);
	MPI_Get_count(status, MPI_CHAR, &s);

	size  = s;
	if (bufferSize < size){
		delete [] buffer;
		bufferSize *= 1.5;
		buffer = new char[bufferSize];
	}

	MPI_Recv(buffer, s, MPI_BYTE, 0, MSG_FDDDATA, MPI_COMM_WORLD, status);	
}

void fastComm::sendFDDDataOwn(unsigned long int id, size_t low, size_t up, int dest){
	std::stringstream sbuffer;

	sbuffer << id << low << up;
	sbuffer.get(buffer, bufferSize);
	sbuffer.seekp(0, std::ios::end);

	MPI_Isend( buffer, sbuffer.tellp(), MPI_BYTE, dest, MSG_FDDDATAOWN, MPI_COMM_WORLD, &req[dest]);
}
void fastComm::recvFDDDataOwn(unsigned long int &id, size_t &low, size_t &up){
	int s = 1024;

	MPI_Recv(buffer, s, MPI_BYTE, 0, MSG_FDDDATAOWN, MPI_COMM_WORLD, status);	
	std::stringstream decode(buffer);

	decode >> id >> low >> up;
}

void fastComm::sendReadFDDFile(unsigned long int &id, std::string & filename, size_t &size, size_t & offset, int dest){
	std::stringstream sbuffer;
	
	sbuffer << id << size << offset << filename;
	sbuffer.get(buffer, bufferSize);
	sbuffer.seekp(0, std::ios::end);

	MPI_Isend( buffer, sbuffer.tellp(), MPI_BYTE, dest, MSG_READFDDFILE, MPI_COMM_WORLD, &req[dest]);
}

void fastComm::recvReadFDDFile(unsigned long int &id, std::string & filename, size_t &size, size_t & offset){
	int s = 1024;

	MPI_Recv(buffer, s, MPI_BYTE, 0, MSG_READFDDFILE, MPI_COMM_WORLD, status);	
	std::stringstream decode(buffer);

	decode >> id >> size >> offset >> filename;
}

void fastComm::sendFinish(){
	for (int i = 1; i < numProcs; ++i){
		MPI_Isend( buffer, 1, MPI_BYTE, i, MSG_FINISH, MPI_COMM_WORLD, &req[i-1]);
	}

	MPI_Waitall( numProcs - 1, req, status);
}
void fastComm::recvFinish(){
	MPI_Recv(buffer, 1, MPI_BYTE, 0, MSG_FINISH, MPI_COMM_WORLD, status);	
}

