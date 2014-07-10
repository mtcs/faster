#ifndef LIBFASTER_FASTCOMM_H
#define LIBFASTER_FASTCOMM_H

#include <string>
#include <mpi.h>
#include <sstream>

class fastComm;

#include "definitions.h"
#include "fastTask.h" 
#include "fastCommBuffer.h" 

enum commMode {
	Local,
	Mesos
};

#define MSG_TASK 		0x0001
#define MSG_CREATEFDD 		0x0002
#define MSG_CREATEIFDD 		0x0003
#define MSG_DESTROYFDD 		0x0004
#define MSG_FDDSETDATAID 	0x0005
#define MSG_FDDSETDATA 		0x0006
#define MSG_FDDSET2DDATAID 	0x0007
#define MSG_FDDSET2DDATASIZES	0x0008
#define MSG_FDDSET2DDATA 	0x0009
#define MSG_READFDDFILE		0x000A
#define MSG_COLLECT		0x000B
#define MSG_FDDDATAID 		0x000C
#define MSG_FDDDATA 		0x000D
#define MSG_TASKRESULT		0x000E
#define MSG_FDDINFO		0x000F
#define MSG_FDDSETIDATAID 	0x0010
#define MSG_FDDSETIDATA		0x0011
#define MSG_FDDSETIKEYS		0x0012
#define MSG_FDDSET2DIDATAID 	0x0013
#define MSG_FDDSET2DIDATASIZES	0x0014
#define MSG_FDDSET2DIDATA 	0x0015
#define MSG_FDDSET2DIKEYS 	0x0016
// . . .
#define MSG_FINISH 		0x8000


#define FDDTYPE_NULL 		0x00
#define FDDTYPE_CHAR 		0x01
#define FDDTYPE_INT 		0x02
#define FDDTYPE_LONGINT 	0x03
#define FDDTYPE_FLOAT 		0x04
#define FDDTYPE_DOUBLE 		0x05
#define FDDTYPE_STRING 		0x07
#define FDDTYPE_CHARP 		0x08
#define FDDTYPE_INTP 		0x09
#define FDDTYPE_LONGINTP 	0x0A
#define FDDTYPE_FLOATP 		0x0B
#define FDDTYPE_DOUBLEP		0x0C
#define FDDTYPE_OBJECT 		0x06





// Communications class
// Responsible for process communication
class fastComm{
	friend class fastContext;
	private:
		MPI_Status * status;
		MPI_Request * req;
		MPI_Request * req2;
		fastCommBuffer * buffer;
		fastCommBuffer buffer2;
		fastCommBuffer buffer3;

		commMode mode;
		int numProcs;
		int procId;
		double timeStart, timeEnd;

		// 1D array
		void sendDataGeneric(unsigned long int id, int dest, void * data, size_t size, int tagID, int tagData);
		void sendIDataGeneric(unsigned long int id, int dest, void * keys, void * data, size_t size, int tagID, int tagKeys, int tagData);
		// 2D array
		void sendDataGeneric(unsigned long int id, int dest, void ** data, size_t * lineSizes, size_t size, size_t itemSize, int tagID, int tagDataSize, int tagData);
		void sendIDataGeneric(unsigned long int id, int dest, void * keys, void ** data, size_t * lineSizes, size_t size, size_t itemSize, int tagID, int tagDataSize, int tagKeys, int tagData);
		// For String and Vector
		template <typename T>
		void sendDataGenericC(unsigned long int id, int dest, T * data, size_t size, int tagID, int tagData);
		template <typename T>
		void sendIDataGenericC(unsigned long int id, int dest, void * keys, T * data, size_t size, int tagID, int tagKeys, int tagData);

		void recvDataGeneric(unsigned long int &id, int src, void *& data, size_t &size, int tagID, int tagData);
		void recvIDataGeneric(unsigned long int &id, int src, void *& keys, void *& data, size_t &size, int tagID, int tagKeys, int tagData);
		void recvDataGeneric(unsigned long int &id, int src, void **& data, size_t *& lineSizes, size_t &size, int tagID, int tagDataSize, int tagData);
		void recvIDataGeneric(unsigned long int &id, int src, void *& keys, void **& data, size_t *& lineSizes, size_t &size, int tagID, int tagDataSize, int tagKeys, int tagData);

	public:
		fastComm(const std::string master);
		~fastComm();

		int getProcId(){ return procId; }

		bool isDriver();

		void probeMsgs(int & tag);
		void waitForReq(int numReqs);
		
		void sendTask(fastTask & task);
		void recvTask(fastTask & task);

		void sendTaskResult(unsigned long int id, void * res, size_t size, double time);
		void * recvTaskResult(unsigned long int &id, int &proc, size_t &size, double &time);

		void sendCreateFDD(unsigned long int id,  fddType type, size_t size, int dest);
		void recvCreateFDD(unsigned long int &id, fddType &type, size_t & size);
		void sendCreateIFDD(unsigned long int id,  fddType kType,  fddType tType,  size_t size, int dest);
		void recvCreateIFDD(unsigned long int &id, fddType &kType, fddType &tType, size_t & size);

		void sendDestroyFDD(unsigned long int id);
		void recvDestroyFDD(unsigned long int &id);

		// Set Data
		void sendFDDSetData(unsigned long int id, int dest, void * data, size_t size);
		void sendFDDSetData(unsigned long int id, int dest, void ** data, size_t * lineSizes, size_t size, size_t itemSize);
		void sendFDDSetData(unsigned long int id, int dest, std::string * data, size_t size);
		template <typename T>
		void sendFDDSetData(unsigned long int id, int dest, std::vector<T> * data, size_t size);

		void sendFDDSetIData(unsigned long int id, int dest, void * keys, void * data, size_t size);
		void sendFDDSetIData(unsigned long int id, int dest, void * keys, void ** data, size_t * lineSizes, size_t size, size_t itemSize);
		void sendFDDSetIData(unsigned long int id, int dest, void * keys, std::string * data, size_t size);
		template <typename T>
		void sendFDDSetIData(unsigned long int id, int dest, void * keys, std::vector<T> * data, size_t size);

		void recvFDDSetData(unsigned long int &id, void *& data, size_t &size);
		void recvFDDSetData(unsigned long int &id, void **& data, size_t *& lineSizes, size_t &size);

		void recvFDDSetIData(unsigned long int &id, void *& keys, void *& data, size_t &size);
		void recvFDDSetIData(unsigned long int &id, void *& keys, void **& data, size_t *& lineSizes, size_t &size);


		// Data
		void sendFDDData(unsigned long int id, int dest, void * data, size_t size);
		void recvFDDData(unsigned long int &id, void * data, size_t &size);


		// Read File
		void sendReadFDDFile(unsigned long int id, std::string filename, size_t size, size_t offset, int dest);
		void recvReadFDDFile(unsigned long int &id, std::string & filename, size_t &size, size_t & offset);

		void sendFDDInfo(size_t size);
		void recvFDDInfo(size_t &size);

		void sendCollect(unsigned long int id);
		void recvCollect(unsigned long int &id);

		void sendFinish();
		void recvFinish();




};

#endif
