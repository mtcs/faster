#ifndef LIBFASTER_FASTCOMM_H
#define LIBFASTER_FASTCOMM_H

#include <string>
#include <mpi.h>
#include <sstream>


#include "definitions.h"
#include "fastCommBuffer.h" 

namespace faster{
	class fastComm;
	class fastTask;

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
		MPI_Request * req3;
		MPI_Request * req4;

		commMode mode;
		int numProcs;
		int procId;
		double timeStart, timeEnd;

		template <typename T>
		void sendDataUltraPlus(int dest, T * data, size_t * lineSizes, size_t size, int tag, fastCommBuffer & b UNUSED, MPI_Request * request);
		void sendDataUltraPlus(int dest, std::string * data, size_t * lineSizes, size_t size, int tag, fastCommBuffer & b UNUSED, MPI_Request * request);
		template <typename T>
		void sendDataUltraPlus(int dest, std::vector<T> * data, size_t * lineSizes, size_t size, int tag, fastCommBuffer & b UNUSED, MPI_Request * request);
		template <typename T>
		void sendDataUltraPlus(int dest, T ** data, size_t * lineSizes, size_t size, int tag, fastCommBuffer & b UNUSED, MPI_Request * request);

		template <typename K, typename T>
		void sendDataUltra(unsigned long int id, int dest, K * keys, T * data, size_t * lineSizes, size_t size, int tagID, int tagDataSize, int tagKeys, int tagData);


		void recvDataUltraPlus(int src, void *& data, int tag, fastCommBuffer & b UNUSED);

		void recvDataUltra(unsigned long int &id, int src, void *& keys, void *& data, size_t *& lineSizes, size_t &size, int tagID, int tagDataSize, int tagKeys, int tagData);


		public:
		fastCommBuffer * buffer;
		fastCommBuffer * buffer3;
		fastCommBuffer  bufferRecv[3];

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
		template <typename T>
		void sendFDDSetData(unsigned long int id, int dest, T * data, size_t size);
		template <typename T>
		void sendFDDSetData(unsigned long int id, int dest, T ** data, size_t * lineSizes, size_t size);

		template <typename K, typename T>
		void sendFDDSetIData(unsigned long int id, int dest, K * keys, T * data, size_t size);
		template <typename K, typename T>
		void sendFDDSetIData(unsigned long int id, int dest, K * keys, T ** data, size_t * lineSizes, size_t size);

		void recvFDDSetData(unsigned long int &id, void *& data, size_t &size);
		void recvFDDSetData(unsigned long int &id, void *& data, size_t *& lineSizes, size_t &size);

		template <typename K, typename T>
		void recvFDDSetIData(unsigned long int &id, K *& keys, T *& data, size_t &size);
		template <typename K, typename T>
		void recvFDDSetIData(unsigned long int &id, K *& keys, T *& data, size_t *& lineSizes, size_t &size);


		// Data
		template <typename T>
		void sendFDDData(unsigned long int id, int dest, T * data, size_t size);
		template <typename K, typename T>
		void sendIFDDData(unsigned long int id, int dest, K * keys, T * data, size_t size);
		void recvFDDData(unsigned long int &id, void * data, size_t &size);
		void recvIFDDData(unsigned long int &id, void * keys, void * data, size_t &size);


		template <typename T>
		void sendFDDDataCollect(unsigned long int id, T * data, size_t size);
		template <typename T>
		void sendFDDDataCollect(unsigned long int id, T ** data, size_t * dataSizes, size_t size);
		template <typename K, typename T>
		void sendFDDDataCollect(unsigned long int id, K * keys, T * data, size_t size);
		template <typename K, typename T>
		void sendFDDDataCollect(unsigned long int id, K * keys, T ** data, size_t * dataSizes, size_t size);


		template <typename T>
		inline void decodeCollect(T & item);
		template <typename T>
		inline void decodeCollect(std::pair<T*,size_t> & item);
		template <typename K, typename T>
		inline void decodeCollect(std::pair<K, T> & item);
		template <typename K, typename T>
		inline void decodeCollect(std::tuple<K, T*, size_t> & item);

		template <typename T>
		void recvFDDDataCollect(std::vector<T> & ret);




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
		template <typename K>
		void sendKeyOwnershipSugest(int dest, K key);
		template <typename K>
		void sendMyKeyOwnership(K key);
		template <typename K>
		void recvKeyOwnershipGeneric(K * keys, int tag);
		template <typename K>
		void recvKeyOwnershipSugest(K * keys);
		template <typename K>
		void recvAllKeyOwnership(K * keys);
		void sendMyKeyCount(int dest, size_t numKeys);
		template <typename K>
		typename std::list<std::pair<K,size_t>>  recvMyKeyCount(int & src);
		template <typename K>
		void sendCountByKey(CountKeyMapT<K> & count);
		template <typename K>
		void recvCountByKey(CountKeyMapT<K> & count);

	};



	/* ------- DATA Serialization -------- */


	template <typename T>
	size_t getSize(  T * data UNUSED, size_t * ds UNUSED, size_t s ){
		return s*sizeof(T);
	}

	template <typename T>
	size_t getSize(  std::vector<T> * data, size_t * ds UNUSED, size_t s ){
		size_t rawDataSize = 0;
		for( size_t i = 0; i < s; ++i ){
			rawDataSize += (sizeof(size_t) + data[i].size()*sizeof(T));
		}
		return s*sizeof(T);
	}

	template <typename T>
	size_t getSize(  T ** data UNUSED, size_t * ds, size_t s ){
		size_t rawDataSize = 0;
		for( size_t i = 0; i < s; ++i ){
			rawDataSize += ds[i]*sizeof(T);
		}
		return rawDataSize;
	}

	// TODO use serialization?
	template <typename T>
	void fastComm::sendDataUltraPlus(int dest, T * data, size_t * lineSizes UNUSED, size_t size, int tag, fastCommBuffer & b UNUSED, MPI_Request * request){
		MPI_Isend( data, size*sizeof(T), MPI_BYTE, dest, tag, MPI_COMM_WORLD, request);
	}

	template <typename T>
	void fastComm::sendDataUltraPlus(int dest, std::vector<T> * data, size_t * lineSizes UNUSED, size_t size, int tag, fastCommBuffer & b UNUSED, MPI_Request * request){
		b.reset();
		b.grow(getSize(data, lineSizes, size));

		for ( size_t i = 0; i < size; ++i){
			b << data[i];
		}
		MPI_Isend( b.data(), b.size(), MPI_BYTE, dest, tag, MPI_COMM_WORLD, request);
	}

	template <typename T>
	void fastComm::sendDataUltraPlus(int dest, T ** data, size_t * lineSizes, size_t size, int tag, fastCommBuffer & b UNUSED, MPI_Request * request){
		b.reset();
		b.grow(getSize(data, lineSizes, size));

		for ( size_t i = 0; i < size; ++i){
			b.write(data[i], lineSizes[i]*sizeof(T));
		}
		MPI_Isend( b.data(), b.size(), MPI_BYTE, dest, tag, MPI_COMM_WORLD, request);
	}

	// Generic Data communication functions
	// Send 1D Data
	template <typename K, typename T>
	void fastComm::sendDataUltra(unsigned long int id, int dest, K * keys, T * data, size_t * lineSizes, size_t size, int tagID, int tagDataSize, int tagKeys, int tagData){

		// Send data information
		buffer[dest].reset();
		buffer[dest] << id << size;
		MPI_Isend( buffer[dest].data(), buffer[dest].size(), MPI_BYTE, dest, tagID , MPI_COMM_WORLD, &req2[dest]);

		// Send subarrays sizes
		if (tagDataSize) 
			MPI_Isend( lineSizes, size*sizeof(size_t), MPI_BYTE, dest, tagDataSize, MPI_COMM_WORLD, &req4[dest-1]);

		// Send Keys
		if (tagKeys) 
			sendDataUltraPlus(dest, keys, NULL, size, tagKeys, buffer3[dest], &req3[dest-1] );

		// Send subarrays
		sendDataUltraPlus(dest, data, lineSizes, size, tagData, buffer[dest], &req[dest-1] );

		//MPI_Waitall( size, localReq, stat);

	}


	// 1D Primitive types
	template <typename T>
	void fastComm::sendFDDSetData(unsigned long int id, int dest, T * data, size_t size){
		int * NULLRef = NULL;
		sendDataUltra(id, dest, NULLRef, data, (size_t*) NULLRef, size, MSG_FDDSETDATAID, 0, 0, MSG_FDDSETDATA);
	}
	template <typename K, typename T>
	void fastComm::sendFDDSetIData(unsigned long int id, int dest, K * keys, T * data, size_t size){
		size_t * NULLRef = NULL;
		sendDataUltra(id, dest, keys, data, NULLRef, size, MSG_FDDSETIDATAID, 0, MSG_FDDSETIKEYS, MSG_FDDSETIDATA);
	}

	// 2D Primitive types
	template <typename T>
	void fastComm::sendFDDSetData(unsigned long int id, int dest, T ** data, size_t *lineSize, size_t size){
		int * NULLRef = NULL;
		sendDataUltra(id, dest, NULLRef, data, lineSize, size, MSG_FDDSET2DDATAID, MSG_FDDSET2DDATASIZES, 0, MSG_FDDSET2DDATA);
	}
	template <typename K, typename T>
	void fastComm::sendFDDSetIData(unsigned long int id, int dest, K * keys, T ** data, size_t *lineSize, size_t size){
		sendDataUltra(id, dest, keys, data, lineSize, size, MSG_FDDSET2DIDATAID, MSG_FDDSET2DIDATASIZES, MSG_FDDSET2DIKEYS, MSG_FDDSET2DIDATA);
	}

	template <typename K, typename T>
	void fastComm::recvFDDSetIData(unsigned long int &id, K *& keys, T *& data, size_t &size){
		void * NULLRef = NULL;
		recvDataUltra(id, 0, keys, data, (size_t*&) NULLRef, size, MSG_FDDSETIDATAID, 0, MSG_FDDSETIKEYS, MSG_FDDSETIDATA);
	}

	template <typename K, typename T>
	void fastComm::recvFDDSetIData(unsigned long int &id, K *& keys, T *& data, size_t *& lineSizes, size_t &size){
		recvDataUltra(id, 0, keys, data, lineSizes, size, MSG_FDDSET2DIDATAID, MSG_FDDSET2DIDATASIZES, MSG_FDDSET2DIKEYS, MSG_FDDSET2DIDATA);
	}


	// Parallel data communication
	template <typename T>
	void fastComm::sendFDDData(unsigned long int id, int dest, T * data, size_t size){
		sendDataUltra<int,T>(id, dest, NULL, data, NULL, size, MSG_FDDDATAID, 0, 0, MSG_FDDDATA);
	}
	template <typename K, typename T>
	void fastComm::sendIFDDData(unsigned long int id, int dest, K * keys, T * data, size_t size){
		sendDataUltra(id, dest, NULL, data, NULL, size, MSG_IFDDDATAID, 0, MSG_IFDDDATAKEYS, MSG_IFDDDATA);
	}

	template <typename T>
	void fastComm::sendFDDDataCollect(unsigned long int id, T * data, size_t size){
		buffer[0].reset();
		buffer[0] << id << size;

		buffer[0].grow(16 + getSize(data, NULL, size));

		for( int i = 0; i < size; ++i ){
			buffer[0] << data[i];
		}

		MPI_Isend( buffer[0].data(), buffer[0].size(), MPI_BYTE, 0, MSG_COLLECTDATA , MPI_COMM_WORLD, req);
	}
	template <typename T>
	void fastComm::sendFDDDataCollect(unsigned long int id, T ** data, size_t * dataSizes, size_t size){

		buffer[0].reset();
		buffer[0] << id << size;

		buffer[0].grow(16 + (size*sizeof(size_t)) + getSize(data, dataSizes, size));

		for( int i = 0; i < size; ++i ){
			buffer[0] << dataSizes[i];
			buffer[0].write(data[i], dataSizes[i]*sizeof(T));
		}

		MPI_Isend( buffer[0].data(), buffer[0].size(), MPI_BYTE, 0, MSG_COLLECTDATA , MPI_COMM_WORLD, req);
	}
	template <typename K, typename T>
	void fastComm::sendFDDDataCollect(unsigned long int id, K * keys, T * data, size_t size){

		buffer[0].reset();
		buffer[0] << id << size;

		buffer[0].grow(16 + getSize(keys, NULL, size) + getSize(data, NULL, size));

		for( int i = 0; i < size; ++i ){
			buffer[0] << keys[i] << data[i];
		}

		MPI_Isend( buffer[0].data(), buffer[0].size(), MPI_BYTE, 0, MSG_COLLECTDATA , MPI_COMM_WORLD, req);
	}
	template <typename K, typename T>
	void fastComm::sendFDDDataCollect(unsigned long int id, K * keys, T ** data, size_t * dataSizes, size_t size){

		buffer[0].reset();
		buffer[0] << id << size;

		buffer[0].grow(16 + getSize(keys, NULL, size) + (size*sizeof(size_t)) + getSize(data, dataSizes, size));

		for( int i = 0; i < size; ++i ){
			buffer[0] << keys[i] << dataSizes[i];
			buffer[0].write(data[i], dataSizes[i]*sizeof(T));
		}
		MPI_Isend( buffer[0].data(), buffer[0].size(), MPI_BYTE, 0, MSG_COLLECTDATA , MPI_COMM_WORLD, req);
	}

	template <typename T>
	inline void fastComm::decodeCollect(T & item){
		bufferRecv[0] >> item;
	}
	template <typename T>
	inline void fastComm::decodeCollect(std::pair<T*,size_t> & item){
		bufferRecv[0] >> item.second;
		item.first = new T[item.second];
		bufferRecv[0].read(item.first, item.second * sizeof(T) );
	}
	template <typename K, typename T>
	inline void fastComm::decodeCollect(std::pair<K, T> & item){
		bufferRecv[0] >> item.first >> item.second;
	}
	template <typename K, typename T>
	inline void fastComm::decodeCollect(std::tuple<K, T*, size_t> & item){
		bufferRecv[0]  >> std::get<0>(item) >> std::get<2>(item);
		std::get<1>(item) = new T[std::get<2>(item)];
		bufferRecv[0].read(std::get<1>(item), std::get<2>(item) * sizeof(T) );
	}

	template <typename T>
	void fastComm::recvFDDDataCollect(std::vector<T> & ret){
		size_t count = 0, size;
		unsigned long int id;
		for (int i = 1; i < (numProcs); ++i){
			bufferRecv[0].reset();
			MPI_Recv(bufferRecv[0].data(), bufferRecv[0].free(), MPI_BYTE, i, MSG_COLLECTDATA, MPI_COMM_WORLD, status);	
			bufferRecv[0] >> id >> size;
			std::cerr << "[" << id << ":" << size<< "] " ;
			for (int j = 0; j < size; ++j){
				decodeCollect(ret[count]);
				count ++;
			}
		}
	}


	// GroupByKey
	// Distribute Key Ownership
	template <typename K>
	void fastComm::sendKeyOwnershipSugest(int dest, K key){
		buffer[dest].reset();
		buffer[dest] << key;
		MPI_Isend(buffer[dest].data(), buffer[dest].free(), MPI_BYTE, dest, MSG_KEYOWNERSHIPSUGEST, MPI_COMM_WORLD, &req[dest-1]);
	}
	template <typename K>
	void fastComm::sendMyKeyOwnership(K key){
		for ( int i = 1; i < (numProcs); ++i){
			if (i != procId){
				buffer[i].reset();
				buffer[i] << key;
				MPI_Isend(buffer[i].data(), buffer[i].free(), MPI_BYTE, i, MSG_MYKEYOWNERSHIP, MPI_COMM_WORLD, &req[i-1]);
			}
		}
	}

	template <typename K>
	void fastComm::recvKeyOwnershipGeneric(K * keys, int tag){
		bufferRecv[0].reset();
		for ( int i = 0; i < (numProcs - 2); ++i){
			MPI_Recv(bufferRecv[0].data(), bufferRecv[0].free(), MPI_BYTE, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, status);	
			bufferRecv[0] >> keys[status[0].MPI_SOURCE];
		}
	}
	template <typename K>
	void fastComm::recvKeyOwnershipSugest(K * keys){
		recvKeyOwnershipGeneric(keys, MSG_KEYOWNERSHIPSUGEST);
	}
	template <typename K>
	void fastComm::recvAllKeyOwnership(K * keys){
		recvKeyOwnershipGeneric(keys, MSG_MYKEYOWNERSHIP);
	}


	template <typename K>
	typename std::list<std::pair<K,size_t>>  fastComm::recvMyKeyCount(int & src){
		size_t numKeys = 0;
		std::list<std::pair<K,size_t>> countList;

		MPI_Recv(bufferRecv[0].data(), bufferRecv[0].free(), MPI_BYTE, MPI_ANY_SOURCE, MSG_KEYOWNERSHIPSUGEST, MPI_COMM_WORLD, status);	
		src = status->MPI_SOURCE;
		bufferRecv[0] >> numKeys;
		for ( int i = 0; i < numKeys; ++i ){
			K k;
			size_t c;
			bufferRecv[0] >> k >> c;
			countList.push_back(std::pair<K,size_t>(k,c));
		}
		return countList;
	}

	template <typename K>
	void fastComm::sendCountByKey(CountKeyMapT<K> & count){

		buffer[0].reset();
		buffer[0] << count.size();

		for ( auto it = count.begin(); it != count.end(); it++ ){
			buffer[0] << it->first << it->second;
			MPI_Isend(buffer[0].data(), buffer[0].free(), MPI_BYTE, 0, MSG_MYKEYCOUNT, MPI_COMM_WORLD, &req[0]);
		}
	}
	template <typename K>
	void fastComm::recvCountByKey(CountKeyMapT<K> & count){
		size_t size;

		buffer[0].reset();
		buffer[0] >> size;

		for ( size_t i = 0; i < size; ++i){
			K key;
			size_t c;
			buffer[0] >> key >> c;
			count[key] = c;
		}
	}
}
#endif
