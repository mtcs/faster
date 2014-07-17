#ifndef LIBFASTER_FASTCOMM_H
#define LIBFASTER_FASTCOMM_H

#include <string>
#include <mpi.h>
#include <sstream>

class fastComm;
class fastTask;

#include "definitions.h"
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
#define MSG_KEYOWNERSHIPSUGEST	0x0017
#define MSG_MYKEYOWNERSHIP	0x0018
#define MSG_MYKEYCOUNT		0x0019
#define MSG_IFDDDATAID 		0x001a
#define MSG_IFDDDATAKEYS	0x001b
#define MSG_IFDDDATA 		0x001c
#define MSG_COLLECTDATA		0x001d
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
		fastCommBuffer * buffer;
		fastCommBuffer buffer2;
		fastCommBuffer buffer3;

		fastComm(const std::string master);
		~fastComm();

		int getProcId(){ return procId; }
		int getNumProcs(){ return numProcs; }

		bool isDriver();

		void probeMsgs(int & tag);
		void waitForReq(int numReqs);
		
		// Task
		void sendTask(fastTask & task);
		void recvTask(fastTask & task);

		void sendTaskResult(unsigned long int id, void * res, size_t size, double time);
		void * recvTaskResult(unsigned long int &id, int &proc, size_t &size, double &time);

		// FDD Creation / Destruction
		void sendCreateFDD(unsigned long int id,  fddType type, size_t size, int dest);
		void recvCreateFDD(unsigned long int &id, fddType &type, size_t & size);
		void sendCreateIFDD(unsigned long int id,  fddType kType,  fddType tType,  size_t size, int dest);
		void recvCreateIFDD(unsigned long int &id, fddType &kType, fddType &tType, size_t & size);

		void sendCreateFDDGroup(unsigned long int id, std::vector<unsigned long int> &idV, std::vector<fddType> &kV, std::vector<fddType> tV);
		void recvCreateFDDGroup(unsigned long int &id, std::vector<unsigned long int> &idV, std::vector<fddType> &kV, std::vector<fddType> tV);

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
		void sendIFDDData(unsigned long int id, int dest, void * keys, void * data, size_t size);
		void recvIFDDData(unsigned long int &id, void * keys, void * data, size_t &size);
		
		

		template <typename T>
		void sendFDDDataCollect(unsigned long int id, T * data, size_t size){
			buffer[0].reset();
			buffer[0] << id << size;
			for( int i = 0; i < size; ++i ){
				buffer[0] << data[i];
			}
			MPI_Isend( buffer[0].data(), buffer[0].size(), MPI_BYTE, 0, MSG_COLLECTDATA , MPI_COMM_WORLD, req);
		}
		template <typename T>
		void sendFDDDataCollect(unsigned long int id, T ** data, size_t * dataSizes, size_t size){
			buffer[0].reset();
			buffer[0] << id << size;
			for( int i = 0; i < size; ++i ){
				buffer[0] << dataSizes[i];
				buffer[0].write(data[i], dataSizes[i]*sizeof(T));
			}
			MPI_Isend( buffer[0].data(), buffer[0].size(), MPI_BYTE, 0, MSG_COLLECTDATA , MPI_COMM_WORLD, req);
		}
		template <typename K, typename T>
		void sendFDDDataCollect(unsigned long int id, K * keys, T * data, size_t size){
			buffer[0].reset();
			buffer[0] << id << size;
			for( int i = 0; i < size; ++i ){
				buffer[0] << keys[i] << data[i];
			}
			MPI_Isend( buffer[0].data(), buffer[0].size(), MPI_BYTE, 0, MSG_COLLECTDATA , MPI_COMM_WORLD, req);
			std::cerr << "Sent";
		}
		template <typename K, typename T>
		void sendFDDDataCollect(unsigned long int id, K * keys, T ** data, size_t * dataSizes, size_t size){
			buffer[0].reset();
			buffer[0] << id << size;
			for( int i = 0; i < size; ++i ){
				buffer[0] << keys[i] << dataSizes[i];
				buffer[0].write(data[i], dataSizes[i]*sizeof(T));
			}
			MPI_Isend( buffer[0].data(), buffer[0].size(), MPI_BYTE, 0, MSG_COLLECTDATA , MPI_COMM_WORLD, req);
		}

		template <typename T>
		void recvFDDDataCollect(std::vector<T> & ret){
			size_t count = 0, size;
			unsigned long int id;
			for (int i = 1; i < (numProcs); ++i){
				buffer2.reset();
				MPI_Recv(buffer2.data(), buffer2.free(), MPI_BYTE, i, MSG_COLLECTDATA, MPI_COMM_WORLD, status);	
				buffer2 >> id >> size;
				std::cerr << "[" << id << ":" << size<< "] " ;
				for (int j = 0; j < size; ++j){
					buffer2 >> ret[count++];
				}
			}
		}
		template <typename T>
		void recvFDDDataCollect(std::vector<std::pair<T*,size_t>> & ret){
			size_t count = 0, size;
			unsigned long int id;
			for (int i = 1; i < (numProcs); ++i){
				buffer2.reset();
				MPI_Recv(buffer2.data(), buffer2.free(), MPI_BYTE, i, MSG_COLLECTDATA, MPI_COMM_WORLD, status);	
				buffer2 >> id >> size;
				std::cerr << "[" << id << ":" << size<< "] " ;
				for (int j = 0; j < size; ++j){
					buffer2 >> ret[count].second;
					ret[count].first = new T[ret[count].second];
					buffer2.read(ret[count].first, ret[count].second * sizeof(T) );
					count ++;
				}
			}
		}
		template <typename K, typename T>
		void recvFDDDataCollect(std::vector<std::pair<K, T>> & ret){
			size_t count = 0, size;
			unsigned long int id;
			for (int i = 1; i < (numProcs); ++i){
				buffer2.reset();
				MPI_Recv(buffer2.data(), buffer2.free(), MPI_BYTE, i, MSG_COLLECTDATA, MPI_COMM_WORLD, status);	
				buffer2 >> id >> size;
				std::cerr << "[" << id << ":" << size<< "] " ;
				for (int j = 0; j < size; ++j){
					buffer2 >> ret[count].first >> ret[count].second;
					count ++;
				}
			}
		}
		template <typename K, typename T>
		void recvFDDDataCollect(std::vector<std::tuple<K, T*, size_t>> & ret){
			size_t count = 0, size;
			unsigned long int id;
			for (int i = 1; i < (numProcs); ++i){
				buffer2.reset();
				MPI_Recv(buffer2.data(), buffer2.free(), MPI_BYTE, i, MSG_COLLECTDATA, MPI_COMM_WORLD, status);	
				buffer2 >> id >> size;
				std::cerr << "[" << id << ":" << size<< "] " ;
				for (int j = 0; j < size; ++j){
					buffer2  >> std::get<0>(ret[count]) >> std::get<2>(ret[count]);
					std::get<1>(ret[count]) = new T[std::get<2>(ret[count])];
					buffer2.read(std::get<1>(ret[count]), std::get<2>(ret[count]) * sizeof(T) );
					count ++;
				}
			}
		}



		// Read File
		void sendReadFDDFile(unsigned long int id, std::string filename, size_t size, size_t offset, int dest);
		void recvReadFDDFile(unsigned long int &id, std::string & filename, size_t &size, size_t & offset);

		void sendFDDInfo(size_t size);
		void recvFDDInfo(size_t &size);

		void sendCollect(unsigned long int id);
		void recvCollect(unsigned long int &id);

		void sendFinish();
		void recvFinish();


		// GroupByKey
		// Distribute Key Ownership
		template <typename K>
		void sendKeyOwnershipSugest(int dest, K key){
			buffer[dest].reset();
			buffer[dest] << key;
			MPI_Isend(buffer[dest].data(), buffer[dest].free(), MPI_BYTE, dest, MSG_KEYOWNERSHIPSUGEST, MPI_COMM_WORLD, &req[dest-1]);
		}
		template <typename K>
		void sendMyKeyOwnership(K key){
			for ( int i = 1; i < (numProcs); ++i){
				if (i != procId){
					buffer[i].reset();
					buffer[i] << key;
					MPI_Isend(buffer[i].data(), buffer[i].free(), MPI_BYTE, i, MSG_MYKEYOWNERSHIP, MPI_COMM_WORLD, &req[i-1]);
				}
			}
		}

		template <typename K>
		void recvKeyOwnershipGeneric(K * keys, int tag){
			buffer2.reset();
			for ( int i = 0; i < (numProcs - 2); ++i){
				MPI_Recv(buffer2.data(), buffer2.free(), MPI_BYTE, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, status);	
				buffer2 >> keys[status[0].MPI_SOURCE];
			}
		}
		template <typename K>
		void recvKeyOwnershipSugest(K * keys){
			recvKeyOwnershipGeneric(keys, MSG_KEYOWNERSHIPSUGEST);
		}
		template <typename K>
		void recvAllKeyOwnership(K * keys){
			recvKeyOwnershipGeneric(keys, MSG_MYKEYOWNERSHIP);
		}

		// Distribute Key Count
		void sendMyKeyCount(int dest, size_t numKeys){
			memcpy(buffer[dest].data(), &numKeys, sizeof(size_t));
			MPI_Isend(buffer[dest].data(), buffer[dest].free(), MPI_BYTE, dest, MSG_MYKEYCOUNT, MPI_COMM_WORLD, &req[dest-1]);
		}

		template <typename K>
		typename std::list<std::pair<K,size_t>>  recvMyKeyCount(int & src){
			size_t numKeys = 0;
			std::list<std::pair<K,size_t>> countList;

			MPI_Recv(buffer2.data(), buffer2.free(), MPI_BYTE, MPI_ANY_SOURCE, MSG_KEYOWNERSHIPSUGEST, MPI_COMM_WORLD, status);	
			src = status->MPI_SOURCE;
			buffer2 >> numKeys;
			for ( int i = 0; i < numKeys; ++i ){
				K k;
				size_t c;
				buffer2 >> k >> c;
				countList.push_back(std::pair<K,size_t>(k,c));
			}
			return countList;
		}

		template <typename K>
		void sendCountByKey(CountKeyMapT<K> & count){
			
			buffer[0].reset();
			buffer[0] << count.size();

			for ( auto it = count.begin(); it != count.end(); it++ ){
				buffer[0] << it->first << it->second;
				MPI_Isend(buffer[0].data(), buffer[0].free(), MPI_BYTE, 0, MSG_MYKEYCOUNT, MPI_COMM_WORLD, &req[0]);
			}
		}
		template <typename K>
		void recvCountByKey(CountKeyMapT<K> & count){
			buffer2.reset();
			size_t size;
			buffer[0] >> size;

			for ( size_t i = 0; i < size; ++i){
				K key;
				size_t c;
				buffer[0] >> key >> c;
				count[key] = c;
			}
		}
};

#endif
