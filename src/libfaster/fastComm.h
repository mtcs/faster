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

#define MSG_TASK 		0x0001
#define MSG_CREATEFDD 		0x0002
#define MSG_DESTROYFDD 		0x0004
#define MSG_FDDSETDATAID 	0x0008
#define MSG_FDDSETDATA 		0x0010
#define MSG_READFDDFILE		0x0020
#define MSG_COLLECT		0x0040
#define MSG_FDDDATAID 		0x0080
#define MSG_FDDDATA 		0x0100
#define MSG_TASKRESULT		0x0200
#define MSG_FDDINFO		0x0400
#define MSG_FINISH 		0x8000

#define FDDTYPE_NULL 		0x00
#define FDDTYPE_CHAR 		0x01
#define FDDTYPE_INT 		0x02
#define FDDTYPE_LONGINT 	0x03
#define FDDTYPE_FLOAT 		0x04
#define FDDTYPE_DOUBLE 		0x05
#define FDDTYPE_STRING 		0x06
#define FDDTYPE_OBJECT 		0x07

#include "fddBase.h" 

char encodeFDDType(fddType type);

class fastCommBuffer{
	private:
		char * _data;
		size_t _size;
		size_t _allocatedSize;
	public:
		fastCommBuffer(){
			reset();
			_allocatedSize = BUFFER_INITIAL_SIZE;
			_data = new char [_allocatedSize];
		}
		~fastCommBuffer(){
			delete [] _data;
		}

		void reset(){ _size = 0; }

		char * data(){ return _data; }
		char * pos(){ return &_data[_size]; }
		size_t size(){ return _size; }
		size_t free(){ return _allocatedSize - _size; }

		void grow(size_t s){
			if (_allocatedSize < s){
				delete [] _data;
				_allocatedSize = std::max(size_t(1.5*_allocatedSize), s + _allocatedSize);
				_data = new char[_allocatedSize];
			}
		}

		void print(){
			for (int i = 0; i < _size; ++i){
				std::cout << (int) _data[i] << ' ';
			}
		}

		// WRITE Data
		template <typename T>
		void write(T v, size_t s){
			memcpy( &_data[_size], &v, s );
			_size += s;
		}

		template <typename T>
		void write(T * v, size_t s){
			memcpy( &_data[_size], v, s );
			_size += s;
		}

		template <typename T>
		void write(T v){
			write( v, sizeof(T) );
		}

		
		// READ Data
		template <typename T>
		void read(T & v, size_t s){
			memcpy(&v, &_data[_size], s );
			_size += s;
		}
		template <typename T>
		void read(T * v, size_t s){
			memcpy(v, &_data[_size], s );
			_size += s;
		}
		template <typename T>
		void read(T & v){
			read( v, sizeof(T) );
		}

		// Operators

		template <typename T>
		fastCommBuffer & operator<<(T v){
			write(v);
			return *this;
		}

		template <typename T>
		fastCommBuffer & operator>>(T & v){
			read(v);
			return *this;
		}
};


// Communications class
// Responsible for process communication
class fastComm{
	friend class fastContext;
	public:
		fastComm(const std::string master);
		~fastComm();

		bool isDriver();

		void probeMsgs(int & tag);
		void waitForReq(int numReqs);
		
		void sendTask(fastTask & task);
		void recvTask(fastTask & task);

		void sendTaskResult(unsigned long int id, void * res, size_t size, double time);
		void recvTaskResult(unsigned long int &id, void * res, size_t &size, double &time);

		void sendCreateFDD(unsigned long int id, fddType type, size_t size);
		void recvCreateFDD(unsigned long int &id, fddType &type, size_t & size);

		void sendDestroyFDD(unsigned long int id);
		void recvDestroyFDD(unsigned long int &id);

		void sendFDDSetData(unsigned long int id, int dest, void * data, size_t size);
		void recvFDDSetData(unsigned long int &id, void *& data, size_t &size);

		void sendFDDData(unsigned long int id, int dest, void * data, size_t size);
		void recvFDDData(unsigned long int &id, void *& data, size_t &size);

		void sendFDDDataOwn(unsigned long int id, size_t low, size_t up, int dest);
		void recvFDDDataOwn(unsigned long int &id, size_t &low, size_t &up);

		void sendReadFDDFile(unsigned long int id, std::string filename, size_t size, size_t offset, int dest);
		void recvReadFDDFile(unsigned long int &id, std::string & filename, size_t &size, size_t & offset);

		void sendFDDInfo(size_t size);
		void recvFDDInfo(size_t &size);

		void sendCollect(unsigned long int id);
		void recvCollect(unsigned long int &id);

		void sendFinish();
		void recvFinish();


	private:
		MPI_Status * status;
		MPI_Request * req;
		MPI_Request * req2;
		fastCommBuffer buffer;

		commMode mode;
		int numProcs;
		int procId;
		double timeStart, timeEnd;


};

#endif
