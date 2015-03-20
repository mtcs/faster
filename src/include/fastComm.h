#ifndef LIBFASTER_FASTCOMM_H
#define LIBFASTER_FASTCOMM_H

#include <string>
#include <mpi.h>
#include <sstream>


#include "definitions.h"
#include "fastCommBuffer.h" 
#include "misc.h"

namespace faster{
	class fastComm;
	class fastTask;

	enum commMode {
		Local,
		Mesos
	};

	/*
	#define MSG_TASK 		0x0001
	#define MSG_CREATEFDD 		0x0002
	#define MSG_CREATEIFDD 		0x0003
	#define MSG_CREATEIFDD 		0x0004
	#define MSG_DISCARDFDD 		0x0005
	#define MSG_FDDSETDATAID 	0x0006
	#define MSG_FDDSETDATA 		0x0007
	#define MSG_FDDSET2DDATAID 	0x0008
	#define MSG_FDDSET2DDATASIZES	0x0009
	#define MSG_FDDSET2DDATA 	0x000A
	#define MSG_READFDDFILE		0x000B
	#define MSG_COLLECT		0x000C
	#define MSG_FDDDATAID 		0x000D
	#define MSG_FDDDATA 		0x000E
	#define MSG_TASKRESULT		0x000F
	#define MSG_FDDINFO		0x0010
	#define MSG_FDDSETIDATAID 	0x0011
	#define MSG_FDDSETIDATA		0x0012
	#define MSG_FDDSETIKEYS		0x0013
	#define MSG_FDDSET2DIDATAID 	0x0014
	#define MSG_FDDSET2DIDATASIZES	0x0015
	#define MSG_FDDSET2DIDATA 	0x0016
	#define MSG_FDDSET2DIKEYS 	0x0017
	#define MSG_KEYOWNERSHIPSUGEST	0x0018
	#define MSG_MYKEYOWNERSHIP	0x0019
	#define MSG_MYKEYCOUNT		0x001a
	#define MSG_IFDDDATAID 		0x001b
	#define MSG_IFDDDATAKEYS	0x001c
	#define MSG_IFDDDATA 		0x001d
	#define MSG_COLLECTDATA		0x001a
	#define MSG_KEYMAP		0x001b
	#define MSG_GROUPBYKEYDATA	0x001c
		// . . .
	#define MSG_FINISH 		0x8000 // */
	enum msgTag : int {
		MSG_TASK 	,
		MSG_CREATEFDD 	,
		MSG_CREATEIFDD 	,
		MSG_CREATEGFDD 	,
		MSG_DISCARDFDD 	,
		MSG_FDDSETDATAID ,
		MSG_FDDSETDATA 	,
		MSG_FDDSET2DDATAID ,
		MSG_FDDSET2DDATASIZES,
		MSG_FDDSET2DDATA ,
		MSG_READFDDFILE	,
		MSG_WRITEFDDFILE,
		MSG_FILENAME,
		MSG_COLLECT	,
		MSG_FDDDATAID 	,
		MSG_FDDDATA 	,
		MSG_TASKRESULT	,
		MSG_FDDINFO	,
		MSG_FDDSETIDATAID ,
		MSG_FDDSETIDATA	,
		MSG_FDDSETIKEYS	,
		MSG_FDDSET2DIDATAID ,
		MSG_FDDSET2DIDATASIZES,
		MSG_FDDSET2DIDATA ,
		MSG_FDDSET2DIKEYS ,
		MSG_KEYOWNERSHIPSUGEST,
		MSG_MYKEYOWNERSHIP,
		MSG_MYKEYCOUNT	,
		MSG_IFDDDATAID 	,
		MSG_IFDDDATAKEYS,
		MSG_IFDDDATA 	,
		MSG_COLLECTDATA	,
		MSG_KEYMAP	,
		MSG_DISTKEYMAP	,
		MSG_GROUPBYKEYDATA,

		MSG_FINISH
	};



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

		MPI_Group slaveGroup;
		MPI_Comm slaveComm;

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


		void recvDataUltraPlus(int src, void *& data, int & size, int tag, fastCommBuffer & b UNUSED);

		void recvDataUltra(unsigned long int &id, int src, void *& keys, void *& data, size_t *& lineSizes, size_t &size, int tagID, int tagDataSize, int tagKeys, int tagData);


		fastCommBuffer * buffer;
		fastCommBuffer * bufferRecv;
		fastCommBuffer resultBuffer;
		public:

		fastComm(int & argc, char **& argv);
		~fastComm();

		int getProcId(){ return procId; }
		int getNumProcs(){ return numProcs; }
		fastCommBuffer & getResultBuffer(){ return resultBuffer; }
		fastCommBuffer * getSendBuffers(){ return buffer; }

		bool isDriver();

		void probeMsgs(int & tag, int & src);
		void waitForReq(int numReqs);
		void joinAll();
		void joinSlaves();

		template <typename T>
		size_t getSize(  T * data UNUSED, size_t * ds UNUSED, size_t s );
		template <typename T>
		size_t getSize(  std::vector<T> * data, size_t * ds UNUSED, size_t s );
		template <typename T>
		size_t getSize(  T ** data UNUSED, size_t * ds, size_t s );
		size_t getSize(  std::string * data, size_t * ds UNUSED, size_t s );

		// Task
		void sendTask(fastTask & task);
		void recvTask(fastTask & task);

		//void sendTaskResult(unsigned long int id, void * res, size_t size, double time);
		void sendTaskResult();
		void * recvTaskResult(unsigned long int &tid, unsigned long int & sid, size_t &size, size_t & time, procstat & stat);

		// FDD Creation / Destruction
		void sendCreateFDD(unsigned long int id,  fddType type, size_t size, int dest);
		void recvCreateFDD(unsigned long int &id, fddType &type, size_t & size);
		void sendCreateIFDD(unsigned long int id,  fddType kType,  fddType tType,  size_t size, int dest);
		void recvCreateIFDD(unsigned long int &id, fddType &kType, fddType &tType, size_t & size);

		void sendCreateFDDGroup(unsigned long int id, fddType keyType, std::vector<unsigned long int> & members);
		void recvCreateFDDGroup(unsigned long int & id, fddType & keyType, std::vector<unsigned long int> & members);

		void sendDiscardFDD(unsigned long int id);
		void recvDiscardFDD(unsigned long int &id);

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
		// THESE FUNCTIONS WILL BE DEPRECATED (THEY WILL BECOME TASKS)
		void sendReadFDDFile(unsigned long int id, std::string filename, size_t size, size_t offset, int dest);
		void recvReadFDDFile(unsigned long int &id, std::string & filename, size_t &size, size_t & offset);
		void sendWriteFDDFile(unsigned long int id,std::string & path, std::string & sufix);
		void recvWriteFDDFile(unsigned long int & id,std::string & path, std::string & sufix);

		void sendFDDInfo(size_t size);
		void recvFDDInfo(size_t &size, int & src);

		void sendFileName(std::string path);
		void recvFileName(std::string & filename);

		void sendCollect(unsigned long int id);
		void recvCollect(unsigned long int &id);

		void sendFinish();
		void recvFinish();
		void bcastBuffer(int src, int i);


		// GroupByKey
		template <typename K>
		void sendKeyMap(unsigned long tid, std::unordered_map<K, int> & keyMap);
		template <typename K>
		void recvKeyMap(unsigned long tid, std::unordered_map<K, int> & keyMap);
		template <typename K>
		void distributeKeyMap(std::unordered_map<K, int> & localKeyMap, std::unordered_map<K, int> & keyMap);
		template <typename K>
		void sendCogroupData(unsigned long tid, std::unordered_map<K, int> & keyMap, std::vector<bool> & flags);
		template <typename K>
		void recvCogroupData(unsigned long tid, std::unordered_map<K, int> & keyMap, std::vector<bool> & flags);

		void sendGroupByKeyData(int i);
		void * recvGroupByKeyData(int &size);

		/*
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
		typename std::deque<std::pair<K,size_t>>  recvMyKeyCount(int & src);
		template <typename K>
		void sendCountByKey(std::unordered_map<K,size_t> & count);
		template <typename K>
		void recvCountByKey(std::unordered_map<K,size_t> & count);
		*/

	};



	/* ------- DATA Serialization -------- */


	template <typename T>
	size_t faster::fastComm::getSize(  T * data UNUSED, size_t * ds UNUSED, size_t s ){
		return s*sizeof(T);
	}

	template <typename T>
	size_t faster::fastComm::getSize(  std::vector<T> * data, size_t * ds UNUSED, size_t s ){
		size_t rawDataSize = 0;
		for( size_t i = 0; i < s; ++i ){
			rawDataSize += (sizeof(size_t) + data[i].size()*sizeof(T));
		}
		return s*sizeof(T);
	}

	template <typename T>
	size_t faster::fastComm::getSize(  T ** data UNUSED, size_t * ds, size_t s ){
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
		MPI_Isend( buffer[dest].data(), buffer[dest].size(), MPI_BYTE, dest, tagID , MPI_COMM_WORLD, &req2[dest-1]);

		// Send subarrays sizes
		if (tagDataSize) 
			MPI_Isend( lineSizes, size*sizeof(size_t), MPI_BYTE, dest, tagDataSize, MPI_COMM_WORLD, &req4[dest-1]);

		// Send Keys
		if (tagKeys){
			sendDataUltraPlus(dest, keys, NULL, size, tagKeys, buffer[dest], &req3[dest-1] );
			MPI_Wait(&req3[dest-1], status);
		}

		// Send subarrays
		sendDataUltraPlus(dest, data, lineSizes, size, tagData, buffer[dest], &req[dest-1] );

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

		for( size_t i = 0; i < size; ++i ){
			buffer[0] << data[i];
		}

		MPI_Isend( buffer[0].data(), buffer[0].size(), MPI_BYTE, 0, MSG_COLLECTDATA , MPI_COMM_WORLD, req);
	}
	template <typename T>
	void fastComm::sendFDDDataCollect(unsigned long int id, T ** data, size_t * dataSizes, size_t size){

		buffer[0].reset();
		buffer[0] << id << size;

		buffer[0].grow(16 + (size*sizeof(size_t)) + getSize(data, dataSizes, size));

		for( size_t i = 0; i < size; ++i ){
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

		for( size_t i = 0; i < size; ++i ){
			buffer[0] << keys[i] << data[i];
		}

		MPI_Isend( buffer[0].data(), buffer[0].size(), MPI_BYTE, 0, MSG_COLLECTDATA , MPI_COMM_WORLD, req);
	}
	template <typename K, typename T>
	void fastComm::sendFDDDataCollect(unsigned long int id, K * keys, T ** data, size_t * dataSizes, size_t size){

		buffer[0].reset();
		buffer[0] << id << size;

		buffer[0].grow(16 + getSize(keys, NULL, size) + (size*sizeof(size_t)) + getSize(data, dataSizes, size));

		for( size_t i = 0; i < size; ++i ){
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
			MPI_Status stat;
			int msgSize = 0;

			MPI_Probe(i, MSG_COLLECTDATA, MPI_COMM_WORLD, &stat);
			MPI_Get_count(&stat, MPI_BYTE, &msgSize);
			bufferRecv[0].reset();
			bufferRecv[0].grow(msgSize);

			MPI_Recv(bufferRecv[0].data(), bufferRecv[0].free(), MPI_BYTE, i, MSG_COLLECTDATA, MPI_COMM_WORLD, &stat);	

			bufferRecv[0] >> id >> size;
			//std::cerr << "[" << id << ":" << size<< "] " ;
			for (size_t j = 0; j < size; ++j){
				decodeCollect(ret[count]);
				count ++;
			}
		}
	}

	// GroupByKey
	template <typename K>
	void faster::fastComm::sendKeyMap(unsigned long tid, std::unordered_map<K, int> & keyMap){
		const int itemsPerMsg = (256 * 1024);
		int num_msgs = ceil( (double) keyMap.size() / itemsPerMsg );
		int inserted_items = 0;
		int msg_num = 0;
		int current_buffer = 0;

		//MPI_Request * requests = new MPI_Request[ num_msgs * (numProcs - 1) ];
		//MPI_Status * statuss = new MPI_Status[ num_msgs * (numProcs - 1) ];

		// Include number of items in first message
		buffer[current_buffer].reset();
		buffer[current_buffer] << tid << size_t(keyMap.size()); 

		std::cerr << " NM:" << num_msgs << "\n";

		for ( auto it = keyMap.begin(); it != keyMap.end(); it++){
			buffer[current_buffer] << it->first << it->second; 
			inserted_items ++;

			// Send some keys
			if (inserted_items >= itemsPerMsg){
				std::cerr << msg_num << " ";
				
				if (msg_num > 0)
					MPI_Waitall( (numProcs - 1), req, status);
				
				for ( int i = 1; i < (numProcs); ++i)
					MPI_Isend(
							buffer[current_buffer].data(), 
							buffer[current_buffer].size(), 
							MPI_BYTE, 
							i, 
							MSG_KEYMAP, 
							MPI_COMM_WORLD, 
							//&requests[ (i-1) + msg_num * (numProcs-1)]
							&req[i-1]
						 );
				buffer[current_buffer].reset();
				inserted_items = 0;
				msg_num++;

				// Next send will use next buffer
				current_buffer = (current_buffer + 1) % numProcs;
			}
		}

		// Send the rest of the keys
		if (inserted_items > 0){
			std::cerr << msg_num << " ";
				
			if (msg_num > 0)
				MPI_Waitall( (numProcs - 1), req, status);
				
			for ( int i = 1; i < (numProcs); ++i)
				MPI_Isend(
						buffer[current_buffer].data(), 
						buffer[current_buffer].size(), 
						MPI_BYTE, 
						i, 
						MSG_KEYMAP, 
						MPI_COMM_WORLD, 
						//&requests[ (i-1) + msg_num * (numProcs-1)]
						&req[i-1]
					 );
		}

		// Wait for message send conclusion
		//MPI_Waitall( num_msgs * (numProcs - 1), requests, statuss);
		MPI_Waitall( (numProcs - 1), req, status);

		//delete [] requests;
		//delete [] statuss;
	}
	template <typename K>
	void faster::fastComm::recvKeyMap(unsigned long tid, std::unordered_map<K, int> & keyMap){
		MPI_Status stat;
		size_t size;
		int rsize;
		
		// Recv initial data
		MPI_Probe(0, MSG_KEYMAP, MPI_COMM_WORLD, &stat);
		MPI_Get_count(&stat, MPI_BYTE, &rsize);
		bufferRecv[0].grow(rsize);
		bufferRecv[0].reset();
		MPI_Recv(bufferRecv[0].data(), 
				bufferRecv[0].free(), 
				MPI_BYTE, 
				0, 
				MSG_KEYMAP, 
				MPI_COMM_WORLD, 
				&stat
			);	

		bufferRecv[0] >> tid >> size;
		
		// Allocate map with pre-known size
		keyMap.reserve(size);

		const int itemsPerMsg = (256 * 1024);
		int num_msgs = ceil( (double) size / itemsPerMsg );
		int inserted_items = 0;
		std::cerr << "(NM:" << num_msgs << ")\n";

		for ( int msg_num = 0; msg_num < num_msgs; ++msg_num){
			int numRevcItems = std::min(size - inserted_items, size_t(itemsPerMsg));
			std::cerr << msg_num << " ";

			// Recv more keys
			if (msg_num > 0){
				MPI_Probe(0, MSG_KEYMAP, MPI_COMM_WORLD, &stat);
				MPI_Get_count(&stat, MPI_BYTE, &rsize);
				bufferRecv[0].grow(rsize);
				bufferRecv[0].reset();
				MPI_Recv(
						bufferRecv[0].data(), 
						bufferRecv[0].free(), 
						MPI_BYTE, 
						0, 
						MSG_KEYMAP, 
						MPI_COMM_WORLD, 
						&stat
					);	
			}

			// Insert recvd keys
			for ( int i = 0; i < numRevcItems; ++i){
				K key;
				int count;
				bufferRecv[0] >> key >> count;
				keyMap[key] = count;
			}
			inserted_items += numRevcItems;
		}
	}

	template <typename K>
	void faster::fastComm::distributeKeyMap(std::unordered_map<K, int> & localKeyMap, std::unordered_map<K, int> & keyMap){
		std::pair<K, int> p;
		keyMap.reserve(localKeyMap.size() * numProcs);

		std::cerr << "---------- Join Slaves ----------";
		joinSlaves();
		std::cerr << "\n";

		for (int i = 1; i < (numProcs); ++i){
			if (procId == i){
				std::cerr << "S" << localKeyMap.size() << " ";
				int reqIndex = 0;
				
				buffer[i] << size_t(localKeyMap.size());
				for ( auto it = localKeyMap.begin(); it != localKeyMap.end(); it++){
					buffer[i] << it->first << it->second;
				}

				for (int j = 1; j < (numProcs); ++j){
					if ( j != i )
						MPI_Isend(buffer[i].data(), buffer[i].size(), MPI_BYTE, j, MSG_DISTKEYMAP, MPI_COMM_WORLD, &req[reqIndex++]); 
				}
				MPI_Waitall( numProcs - 2, req, status);
			}else{
				int rsize;
				size_t numItems;
				MPI_Status stat;

				MPI_Probe(i, MSG_DISTKEYMAP, MPI_COMM_WORLD, &stat);
				MPI_Get_count(&stat, MPI_BYTE, &rsize);
				bufferRecv[i].grow(rsize);

				bufferRecv[i].reset();

				MPI_Recv(bufferRecv[i].data(), bufferRecv[i].free(), MPI_BYTE, i, MSG_DISTKEYMAP, MPI_COMM_WORLD, &stat);	

				bufferRecv[i] >> numItems;
				std::cerr << "R:" << numItems << " ";

				for (size_t j = 0; j < (numItems); ++j){
					bufferRecv[i] >> p.first >> p.second;
					keyMap.insert(std::move(p));
				}
			}
			
		}
		keyMap.insert(localKeyMap.begin(), localKeyMap.end());
		std::cerr << "    Final Size: " << keyMap.size() << "\n";
	}

	template <typename K>
	void faster::fastComm::sendCogroupData(unsigned long tid, std::unordered_map<K, int> & keyMap, std::vector<bool> & flags){
		buffer[0].reset();
		buffer[0] << tid << size_t(keyMap.size()); 

		for ( auto it = keyMap.begin(); it != keyMap.end(); it++){
			buffer[0] << it->first << it->second; 
		}

		buffer[0] << int(flags.size());
		for ( size_t i = 0; i < flags.size(); ++i){
			buffer[0] << char(flags[i]);
		}

		for ( int i = 1; i < (numProcs); ++i)
			MPI_Isend(buffer[0].data(), buffer[0].size(), MPI_BYTE, i, MSG_KEYMAP, MPI_COMM_WORLD, &req[i-1]);
		MPI_Waitall( numProcs - 1, req, status);

		//bcastBuffer(0, 0);

	}
	template <typename K>
	void faster::fastComm::recvCogroupData(unsigned long tid, std::unordered_map<K, int> & keyMap, std::vector<bool> & flags){
		MPI_Status stat;
		size_t size;
		int rsize;
		
		MPI_Probe(0, MSG_KEYMAP, MPI_COMM_WORLD, &stat);
		MPI_Get_count(&stat, MPI_BYTE, &rsize);
		bufferRecv[0].grow(rsize);

		bufferRecv[0].reset();
		
		MPI_Recv(bufferRecv[0].data(), bufferRecv[0].free(), MPI_BYTE, 0, MSG_KEYMAP, MPI_COMM_WORLD, &stat);	
		//bcastBuffer(0, 0);

		bufferRecv[0] >> tid >> size;
		
		// Allocate map with pre-known size
		keyMap.reserve(size);

		for ( size_t i = 0; i < size; ++i){
			K key;
			int count;
			bufferRecv[0] >> key >> count;
			keyMap[key] = count;
		}

		int numFlags = 0;
		bufferRecv[0] >> numFlags;
		flags.resize(numFlags);

		for ( int i = 0; i < numFlags; i++ ){
			char flag;
			bufferRecv[0] >> flag;
			flags[i] = bool(flag);
		}
	}


}
#endif
