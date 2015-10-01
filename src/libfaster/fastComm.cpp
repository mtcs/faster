#include <algorithm>

#include "fastComm.h"
#include "fastTask.h" 

bool faster::fastComm::isDriver(){
	return (procId == 0);
}

faster::fastComm::fastComm(int & argc, char **& argv){
	MPI_Init (&argc, &argv);
	MPI_Comm_size (MPI_COMM_WORLD, &numProcs);
	MPI_Comm_rank (MPI_COMM_WORLD, &procId);
	std::vector<int> slaveIDs(numProcs-1);
	std::iota(slaveIDs.begin(), slaveIDs.end(), 1);

	MPI_Group origGroup;
	MPI_Comm_group(MPI_COMM_WORLD, &origGroup); 
	MPI_Group_incl(origGroup, numProcs-1, slaveIDs.data(), &slaveGroup);
	MPI_Comm_create(MPI_COMM_WORLD, slaveGroup, &slaveComm);
	
	timeStart = MPI_Wtime();

	status = new MPI_Status [numProcs];
	req = new MPI_Request [numProcs];
	req2 = new MPI_Request [numProcs];
	req3 = new MPI_Request [numProcs];
	req4 = new MPI_Request [numProcs];
	buffer = new fastCommBuffer [numProcs];
	bufferRecv = new fastCommBuffer [std::max(3, numProcs)];
	bufferBusy.resize(numProcs);
}

faster::fastComm::~fastComm(){
	timeEnd = MPI_Wtime();
	MPI_Finalize (); 

	delete [] status;
	delete [] req;
	delete [] req2;
	delete [] req3;
	delete [] req4;
	delete [] buffer;
	delete [] bufferRecv;
}

void faster::fastComm::probeMsgs(int & tag, int & src){
	MPI_Status stat;
	MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &stat);
	tag = stat.MPI_TAG;
	src = stat.MPI_SOURCE;
}

void faster::fastComm::waitForReq(int numReqs){
	MPI_Waitall(numReqs, req, status);
}

void faster::fastComm::joinAll(){
	MPI_Barrier(MPI_COMM_WORLD);
}

void faster::fastComm::joinSlaves(){
	MPI_Barrier(slaveComm);
}

void faster::fastComm::sendTask(fastTask &task){
	buffer[0].reset();
	buffer[0].grow(48 + 8*task.globals.size() );

	buffer[0] << task.id << task.operationType << task.srcFDD << task.destFDD << task.functionId;
	buffer[0] << size_t(task.globals.size());

	for ( size_t i = 0; i < task.globals.size(); i++){
		size_t s = std::get<1>(task.globals[i]);
		int type = std::get<2>(task.globals[i]);
		//std::cerr << "SEND Glob.: "<< i <<"T:" << type << " S:" << s << "\n";
		buffer[0] << type;
		buffer[0] << s;
		buffer[0].write(std::get<0>(task.globals[i]), s);
	}

	for (int i = 1; i < numProcs; ++i){
		MPI_Isend(buffer[0].data(), buffer[0].size(), MPI_BYTE, i, MSG_TASK, MPI_COMM_WORLD, &req[i-1]);
	}
	//MPI_Waitall( numProcs - 1, req, status);
	MPI_Waitall( numProcs - 1, req, status);
}

void faster::fastComm::recvTask(fastTask & task){
	MPI_Status stat;
	size_t numGlobals = 0;

	bufferRecv[0].reset();

	MPI_Recv(bufferRecv[0].data(), bufferRecv[0].free(), MPI_BYTE, 0, MSG_TASK, MPI_COMM_WORLD, &stat);	
	
	bufferRecv[0] >> task.id >> task.operationType >> task.srcFDD >> task.destFDD >> task.functionId;

	bufferRecv[0] >> numGlobals;
	for ( size_t i = 0; i < numGlobals; i++){
		size_t varSize = 0;
		char * var;
		int varType;
		
		bufferRecv[0] >> varType;
		bufferRecv[0] >> varSize;

		//std::cerr << "RGlob.: T:" << varType << " S:" << varSize << "\n";

		var = new char[varSize]();
		bufferRecv[0].read(var, varSize);

		task.globals.insert(task.globals.end(), std::make_tuple(var, varSize, varType));
	}
}

//void faster::fastComm::sendTaskResult(unsigned long int id, void * res, size_t size, double time){
void faster::fastComm::sendTaskResult(){

	//std::cerr << "     Result Buffer Size: " << resultBuffer.size() << "\n";
	MPI_Send(resultBuffer.data(), resultBuffer.size() , MPI_BYTE, 0, MSG_TASKRESULT, MPI_COMM_WORLD);
}

void * faster::fastComm::recvTaskResult(unsigned long int & id, unsigned long int & sid, size_t & size, size_t & time, procstat & stat){
	int rsize;

	MPI_Probe(MPI_ANY_SOURCE, MSG_TASKRESULT, MPI_COMM_WORLD, status);
	MPI_Get_count(status, MPI_BYTE, &rsize);
	sid = status->MPI_SOURCE;
	//std::cerr << "     Recv Msg Size: " << rsize << " from " << sid << "\n";

	bufferRecv[sid].grow(rsize);
	bufferRecv[sid].reset();

	MPI_Irecv(bufferRecv[sid].data(), bufferRecv[sid].free(), MPI_BYTE, sid, MSG_TASKRESULT, MPI_COMM_WORLD, &req[sid-1]);	
	
	bufferRecv[sid] >> id >> time >> stat >> size;

	return bufferRecv[sid].pos();
}

void faster::fastComm::sendCreateFDD(unsigned long int id, fddType type, size_t size, int dest){
	//char typeC = encodeFDDType(type);

	buffer[dest].reset();

	//buffer[dest] << id << typeC << size;
	buffer[dest] << id << type << size;

	//std::cerr << '(' << id << ' ' << (int) typeC << ":" << buffer[dest].size() << ')';

	MPI_Isend(buffer[dest].data(), buffer[dest].size(), MPI_BYTE, dest, MSG_CREATEFDD, MPI_COMM_WORLD, &req[dest-1]);
}

void faster::fastComm::recvCreateFDD(unsigned long int &id, fddType &type, size_t &size){
	//char t;
	MPI_Status stat;

	bufferRecv[0].reset();

	MPI_Recv(bufferRecv[0].data(), bufferRecv[0].free(), MPI_BYTE, 0, MSG_CREATEFDD, MPI_COMM_WORLD, &stat);	

	//bufferRecv[0] >> id >> t >> size;
	//type = decodeFDDType(t);
	bufferRecv[0] >> id >> type >> size;

	//std::cerr << '(' << id << ' ' << (int) t << ":" << buffer[dest].size()  << ')';
}

void faster::fastComm::sendCreateIFDD(unsigned long int id, fddType kType,  fddType tType,  size_t size, int dest){
	//char typeC = encodeFDDType(type);

	buffer[dest].reset();
	buffer[dest].grow(32);

	//buffer[dest] << id << typeC << size;
	buffer[dest] << id << kType << tType << size;

	//std::cerr << '(' << id << ' ' << (int) typeC << ":" << buffer[dest].size() << ')';

	MPI_Isend(buffer[dest].data(), buffer[dest].size(), MPI_BYTE, dest, MSG_CREATEIFDD, MPI_COMM_WORLD, &req[dest-1]);
}

void faster::fastComm::recvCreateIFDD(unsigned long int &id, fddType &kType, fddType &tType,  size_t &size){
	//char t;
	MPI_Status stat;

	bufferRecv[0].reset();
	bufferRecv[0].grow(32);

	MPI_Recv(bufferRecv[0].data(), bufferRecv[0].free(), MPI_BYTE, 0, MSG_CREATEIFDD, MPI_COMM_WORLD, &stat);	

	//bufferRecv[0] >> id >> t >> size;
	//type = decodeFDDType(t);
	bufferRecv[0] >> id >> kType >> tType >> size;

	//std::cerr << '(' << id << ' ' << (int) t << ":" << buffer[dest].size()  << ')';
}

void faster::fastComm::sendCreateFDDGroup(unsigned long int id, fddType keyType, std::vector<unsigned long int> & idV){
	for (int dest = 1; dest < numProcs; ++dest){
		buffer[dest].reset();

		buffer[dest] << id << keyType << size_t(idV.size());

		for ( size_t i = 0; i < idV.size(); ++i){
			buffer[dest] << idV[i];
		}

		MPI_Isend(buffer[dest].data(), buffer[dest].size(), MPI_BYTE, dest, MSG_CREATEGFDD, MPI_COMM_WORLD, &req[dest-1]);
	}
	MPI_Waitall( numProcs - 1, req, status);
}
void faster::fastComm::recvCreateFDDGroup(unsigned long int & id, fddType & keyType, std::vector<unsigned long int> & idV){
	size_t numMembers;
	MPI_Status stat;
	bufferRecv[0].reset();

	MPI_Recv(bufferRecv[0].data(), bufferRecv[0].free(), MPI_BYTE, 0, MSG_CREATEGFDD, MPI_COMM_WORLD, &stat);	

	bufferRecv[0] >> id >> keyType >> numMembers;

	idV.resize(numMembers);

	for ( size_t i = 0; i < numMembers; ++i){
		bufferRecv[0] >> idV[i];
	}
}

void faster::fastComm::sendDiscardFDD(unsigned long int id){
	for (int i = 1; i < numProcs; ++i){
		MPI_Isend( &id, sizeof(long unsigned int), MPI_BYTE, i, MSG_DISCARDFDD, MPI_COMM_WORLD, &req[i-1]);
	}

	//MPI_Waitall( numProcs - 1, req, status);
}
void faster::fastComm::recvDiscardFDD(unsigned long int &id){
	MPI_Status stat;
	MPI_Recv(&id, sizeof(long unsigned int), MPI_BYTE, 0, MSG_DISCARDFDD, MPI_COMM_WORLD, &stat);	
}

size_t faster::fastComm::getSize(  std::string * data, size_t * ds UNUSED, size_t s ){
	size_t rawDataSize = 0;
	for( size_t i = 0; i < s; ++i ){
		rawDataSize += (sizeof(size_t) + data[i].size());
	}
	return rawDataSize;
}


void faster::fastComm::sendDataUltraPlus(int dest, std::string * data, size_t * lineSizes UNUSED, size_t size, int tag, fastCommBuffer & b UNUSED, MPI_Request * request){
	b.reset();
	b.grow(getSize(data, lineSizes, size));

	for ( size_t i = 0; i < size; ++i){
		b << data[i];
	}
	MPI_Isend( b.data(), b.size(), MPI_BYTE, dest, tag, MPI_COMM_WORLD, request);
}

void faster::fastComm::recvDataUltraPlus(int src, void *& data, int & size, int tag, fastCommBuffer & b){
	MPI_Status stat, stat2;
	MPI_Probe(src, tag, MPI_COMM_WORLD, &stat);
	MPI_Get_count(&stat, MPI_BYTE, &size);
	b.grow(size);
	b.reset();
	MPI_Recv(b.data(), b.free(), MPI_BYTE, src, tag, MPI_COMM_WORLD, &stat2);	
	data = b.data();
}

// Receive 2D data
void faster::fastComm::recvDataUltra(unsigned long int &id, int src, void *& keys, void *& data, size_t *& lineSizes, size_t &size, int tagID, int tagDataSize, int tagKeys, int tagData){
	int rSize = 0;

	// Receive the FDD ID, size and item size
	bufferRecv[0].reset();
	MPI_Recv(bufferRecv[0].data(), bufferRecv[0].free(), MPI_BYTE, src, tagID, MPI_COMM_WORLD, status);	
	bufferRecv[0] >> id >> size;
		//std::cerr << ".";

	// Receive the size of every line to be received
	if (tagDataSize)
		recvDataUltraPlus(src, (void*&) lineSizes, rSize, tagDataSize, bufferRecv[2]);
		//std::cerr << ".";

	// Receive Keys
	if (tagKeys)
		recvDataUltraPlus(src, keys, rSize, tagKeys, bufferRecv[1]);
		//std::cerr << ".";

	
	recvDataUltraPlus(src, data, rSize, tagData, bufferRecv[0]);
	//std::cerr << ".";
}


void faster::fastComm::recvFDDSetData(unsigned long int &id, void *& data, size_t &size){
	void * NULLRef = NULL;
	//recvDataGeneric(id, 0, data, size, MSG_FDDSETDATAID, MSG_FDDSETDATA);
	recvDataUltra(id, 0, NULLRef, data, (size_t*&) NULLRef, size, MSG_FDDSETDATAID, 0, 0, MSG_FDDSETDATA);
}
void faster::fastComm::recvFDDSetData(unsigned long int &id, void *& data, size_t *& lineSizes, size_t &size){
	//recvDataGeneric(id, 0, data, lineSizes, size, MSG_FDDSET2DDATAID, MSG_FDDSET2DDATASIZES, MSG_FDDSET2DDATA);
	void * NULLRef = NULL;
	recvDataUltra(id, 0, NULLRef, data, lineSizes, size, MSG_FDDSET2DDATAID, MSG_FDDSET2DDATASIZES, 0, MSG_FDDSET2DDATA);
}

void faster::fastComm::recvFDDData(unsigned long int &id, void * data, size_t &size){
	void * NULLRef = NULL;
	//recvDataGeneric(id, 0, data, size, MSG_FDDDATAID, MSG_FDDDATA);
	recvDataUltra(id, 0, NULLRef, data, (size_t*&) NULLRef, size, MSG_FDDDATAID, 0, 0, MSG_FDDDATA);
}
void faster::fastComm::recvIFDDData(unsigned long int &id, void * keys, void * data, size_t &size){
	size_t * NULLRef = NULL;
	//recvIDataGeneric(id, 0, keys, data, size, MSG_IFDDDATAID, MSG_IFDDDATAKEYS, MSG_IFDDDATA);
	recvDataUltra(id, 0, keys, data, NULLRef, size, MSG_IFDDDATAID, 0, MSG_IFDDDATAKEYS, MSG_IFDDDATA);
}



void faster::fastComm::sendReadFDDFile(unsigned long int id, std::string filename, size_t size, size_t offset, int dest){
	
	buffer[dest].reset();

	buffer[dest] << id << size << offset << filename;

	MPI_Isend( buffer[dest].data(), buffer[dest].size(), MPI_BYTE, dest, MSG_READFDDFILE, MPI_COMM_WORLD, &req[dest-1]);
}

void faster::fastComm::recvReadFDDFile(unsigned long int &id, std::string & filename, size_t &size, size_t & offset){
	MPI_Status stat;
	//size_t filenameSize;
	int msgSize = 0;

	bufferRecv[0].reset();
	MPI_Probe(0, MSG_READFDDFILE, MPI_COMM_WORLD, &stat);
	MPI_Get_count(&stat, MPI_BYTE, &msgSize);
	bufferRecv[0].grow(msgSize);

	MPI_Recv(bufferRecv[0].data(), bufferRecv[0].free(), MPI_BYTE, 0, MSG_READFDDFILE, MPI_COMM_WORLD, &stat);	
	//buffer[0] >> id >> size >> offset >> filenameSize;
	//buffer[0].read(filename, filenameSize);
	bufferRecv[0] >> id >> size >> offset >> filename;
	//std::cerr << filename << "\n"; 
}

void faster::fastComm::sendWriteFDDFile(unsigned long int id,std::string & path, std::string & sufix){

	for (int dest = 1; dest < numProcs; ++dest){
		buffer[dest].reset();

		buffer[dest] << id << path << sufix;

		MPI_Isend( buffer[dest].data(), buffer[dest].size(), MPI_BYTE, dest, MSG_WRITEFDDFILE, MPI_COMM_WORLD, &req[dest-1]);
	}
	MPI_Waitall( numProcs - 1, req, status);
}

void faster::fastComm::recvWriteFDDFile(unsigned long int & id,std::string & path, std::string & sufix){
	MPI_Status stat;
	//size_t filenameSize;

	int msgSize = 0;
	bufferRecv[0].reset();
	MPI_Probe(0, MSG_WRITEFDDFILE, MPI_COMM_WORLD, &stat);
	MPI_Get_count(&stat, MPI_BYTE, &msgSize);
	bufferRecv[0].grow(msgSize);

	MPI_Recv(bufferRecv[0].data(), bufferRecv[0].free(), MPI_BYTE, 0, MSG_WRITEFDDFILE, MPI_COMM_WORLD, &stat);	
	//buffer[0] >> id >> size >> offset >> filenameSize;
	//buffer[0].read(filename, filenameSize);
	bufferRecv[0] >> id >> path >> sufix;
	//std::cerr << filename << "\n"; 
}



void faster::fastComm::sendFDDInfo(size_t size){
	MPI_Send( &size, sizeof(size_t), MPI_BYTE, 0, MSG_FDDINFO, MPI_COMM_WORLD);
}

void faster::fastComm::recvFDDInfo(size_t &size, int & src){
	MPI_Recv(&size, sizeof(size_t), MPI_BYTE, MPI_ANY_SOURCE, MSG_FDDINFO, MPI_COMM_WORLD, status);
	src = status[0].MPI_SOURCE;
}

void faster::fastComm::sendFileName(std::string path){
	buffer[0].reset();

	buffer[0] << path;

	for (int dest = 1; dest < numProcs; ++dest){
		MPI_Isend( 
				buffer[0].data(), 
				buffer[0].size(), 
				MPI_BYTE, 
				dest, 
				MSG_FILENAME, 
				MPI_COMM_WORLD, 
				&req[dest-1]);
	}
}

void faster::fastComm::recvFileName(std::string & path){
	MPI_Status stat;

	int msgSize = 0;
	bufferRecv[0].reset();
	MPI_Probe(0, MSG_FILENAME, MPI_COMM_WORLD, &stat);
	MPI_Get_count(&stat, MPI_BYTE, &msgSize);
	bufferRecv[0].grow(msgSize);

	MPI_Recv(
			bufferRecv[0].data(), 
			bufferRecv[0].free(), 
			MPI_BYTE, 
			0, 
			MSG_FILENAME, 
			MPI_COMM_WORLD, 
			&stat);	
	//buffer[0] >> id >> size >> offset >> filenameSize;
	//buffer[0].read(filename, filenameSize);
	bufferRecv[0] >> path;
}

bool faster::fastComm::isSendBufferFree(int i){
	if (bufferBusy[i]){
		int savepoint = i - 1;
		int flag;

		if ( i > procId ){
			savepoint -= 1;
		}

		// check if buffer has been freed
		MPI_Test(&req[savepoint], &flag, MPI_STATUS_IGNORE);
		if (flag){
			bufferBusy[i] = false;
			return true;
		}else{
			bufferBusy[i] = true;
			return false;
		}
	}
	return true;
}

void faster::fastComm::sendGroupByKeyData(int i){
	int savepoint = i - 1;
	if ( i > procId ){
		savepoint -= 1;
	}
	MPI_Isend( 
			buffer[i].data(), 
			buffer[i].size(), 
			MPI_BYTE, 
			i, 
			MSG_GROUPBYKEYDATA, 
			MPI_COMM_WORLD, 
			&req[savepoint]);
	bufferBusy[i] = true;
}

void * faster::fastComm::recvGroupByKeyData(int & size){
	MPI_Status stat;
	size = 0;
	int flag = 0;
	void * data;
	
	MPI_Iprobe(
			MPI_ANY_SOURCE, 
			MSG_GROUPBYKEYDATA, 
			MPI_COMM_WORLD, 
			&flag, 
			&stat
			);
	if (flag == true){
		recvDataUltraPlus(
				MPI_ANY_SOURCE, 
				data, 
				size, 
				MSG_GROUPBYKEYDATA, 
				bufferRecv[0]
				);
	}

	return data;
}



void faster::fastComm::sendCollect(unsigned long int id){
	for (int i = 1; i < numProcs; ++i){
		MPI_Isend( 
				&id, 
				sizeof(long unsigned int), 
				MPI_BYTE, 
				i, 
				MSG_COLLECT, 
				MPI_COMM_WORLD, 
				&req[i-1]
				);
	}

	MPI_Waitall( numProcs - 1, req, status);
}

void faster::fastComm::recvCollect(unsigned long int &id){
	MPI_Status stat;
	MPI_Recv(&id, sizeof(long unsigned int), MPI_BYTE, 0, MSG_COLLECT, MPI_COMM_WORLD, &stat);	
}

void faster::fastComm::sendFinish(){
	for (int i = 1; i < numProcs; ++i){
		MPI_Isend( buffer[0].data(), 1, MPI_BYTE, i, MSG_FINISH, MPI_COMM_WORLD, &req[i-1]);
	}

	MPI_Waitall( numProcs - 1, req, status);
}
void faster::fastComm::recvFinish(){
	MPI_Status stat;
	MPI_Recv(bufferRecv[0].data(), 1, MPI_BYTE, 0, MSG_FINISH, MPI_COMM_WORLD, &stat);	
}

void faster::fastComm::bcastBuffer(int src, int i){
	joinAll();
	MPI_Bcast(buffer[i].data(), buffer[i].size(), MPI_BYTE, src, MPI_COMM_WORLD);
}

