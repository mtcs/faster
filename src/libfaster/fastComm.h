#ifndef LIBFASTER_FASTCOMM_H
#define LIBFASTER_FASTCOMM_H

#include <string>
#include <mpi.h>
#include <sstream>

class fastComm;

enum commMode {
	Local,
	Mesos
};

#define BUFFER_INITIAL_SIZE 4*1024*1024

#define MSG_TASK 	0x0001
#define MSG_CREATEFDD 	0x0002
#define MSG_DESTROYFDD 	0x0004
#define MSG_FDDDATA 	0x0008
#define MSG_FDDDATAID 	0x0010
#define MSG_FDDDATAOWN 	0x0020
#define MSG_READFDDFILE	0x0040
#define MSG_FINISH 	0x8000

#define FDDTYPE_NULL 		0x00
#define FDDTYPE_INT 		0x01
#define FDDTYPE_LONGINT 	0x02
#define FDDTYPE_FLOAT 		0x03
#define FDDTYPE_DOUBLE 		0x04
#define FDDTYPE_STRING 		0x05
#define FDDTYPE_OBJECT 		0x06

#include "fddBase.h" 

char encodeFDDType(fddType & type);

// Communications class
// Responsible for process communication
class fastComm{
	public:
		fastComm(const std::string master);
		~fastComm();

		bool isDriver();

		void probeMsgs(int & tag);
		void fasComm::waitForReq(int numReqs);
		
		void sendTask(fastTask * &t);
		void recvTask(fastTask & task);

		void sendTaskResult(unsigned long int id, void * res, double time);
		void recvTaskResult(unsigned long int id, void * res, double & time);

		void sendCreateFDD(unsigned long int id, fddType type);
		void recvCreateFDD(unsigned long int &id, fddType &type);

		void sendDestroyFDD(unsigned long int id);
		void recvDestroyFDD(unsigned long int &id);

		void sendFDDData(unsigned long int id, int dest, void * data, size_t size);
		void recvFDDData(unsigned long int &id, void * data, size_t &size);

		void sendFDDDataOwn(unsigned long int id, size_t low, size_t up, int dest;
		void recvFDDDataOwn(unsigned long int &id, size_t &low, size_t &up);

		void sendReadFDDFile(unsigned long int &id, std::string & filename, size_t &size, size_t & offset, int dest);
		void recvReadFDDFile(unsigned long int &id, std::string & filename, size_t &size, size_t & offset);

		void sendFinish();
		void recvFinish();

		template <typename T> T waitForResult(unsigned long int tid){
		}

	private:
		MPI_Status * status;
		MPI_Request * req;
		char * buffer;
		size_t bufferSize;

		commMode mode;
		int numProcs;
		int procId;
		double timeStart, timeEnd;

};

#endif
