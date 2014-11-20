#ifndef LIBFASTER_FASTCONTEXT_H
#define LIBFASTER_FASTCONTEXT_H

#include <string>
#include <vector>
#include <queue>
#include <typeinfo>
#include <tuple>
#include <math.h>
#include <chrono>
#include <string>

#include "definitions.h"
#include "fddBase.h"
#include "fastComm.h"
#include "fastScheduler.h"

using std::chrono::system_clock;

namespace faster{

	template <typename T> 
	class fdd;

	template <typename K, typename T> 
	class indexedFdd;

	class fastTask;
	class fastContext;


	class fastSettings{
		friend class fastContext;
		public:

			fastSettings() { 
				_allowDataBalancing = false;
			}

			fastSettings(const fastSettings & s UNUSED){
			}

			void allowDataBalancing(){
				_allowDataBalancing = true;
			}

		private:
			bool _allowDataBalancing;

	};

	// General context
	// Manages the driver and the Workers
	class fastContext{

		template <class T> friend class fdd;
		template <class T> friend class fddCore;
		template <class K, class T> friend class iFddCore;
		template <class K, class T> friend class indexedFdd;
		template <class K> friend class groupedFdd;
		friend class worker;

		public:
			fastContext( int & argc, char **& argv): fastContext(fastSettings(), argc, argv){}
			fastContext( const fastSettings & s, int & argc, char **& argv);
			~fastContext();

			void registerFunction(void * funcP);
			void registerFunction(void * funcP, const std::string name);
			template <class T>
			void registerGlobal(T * varP);

			void startWorkers();

			void calibrate();

			void printInfo();
			void printHeader();
			void updateInfo();

		private:
			int id;
			unsigned long int numFDDs;
			//unsigned long int numTasks;
			fastSettings * settings;
			std::vector< fddBase * > fddList;
			std::vector<void*> funcTable;
			std::vector<std::string> funcName;
			std::vector< std::pair<void*, size_t> > globalTable;
			fastComm * comm;
			fastScheduler * scheduler;

			//std::vector<fastTask *> taskList;

			int findFunc(void * funcP);

			unsigned long int _createFDD(fddBase * ref, fddType type, const std::vector<size_t> * dataAlloc);
			unsigned long int _createIFDD(fddBase * ref, fddType kTypeCode, fddType tTypeCode, const std::vector<size_t> * dataAlloc);
			unsigned long int createFDD(fddBase * ref, size_t typeCode);
			unsigned long int createFDD(fddBase * ref, size_t typeCode, const std::vector<size_t> & dataAlloc);
			unsigned long int createPFDD(fddBase * ref, size_t typeCode);
			unsigned long int createPFDD(fddBase * ref, size_t typeCode, const std::vector<size_t> & dataAlloc);
			unsigned long int createIFDD(fddBase * ref, size_t kTypeCode, size_t tTypeCode);
			unsigned long int createIFDD(fddBase * ref, size_t kTypeCode, size_t tTypeCode, const std::vector<size_t> & dataAlloc);
			unsigned long int createIPFDD(fddBase * ref, size_t kTypeCode, size_t tTypeCode);
			unsigned long int createIPFDD(fddBase * ref, size_t kTypeCode, size_t tTypeCode, const std::vector<size_t> & dataAlloc);
			unsigned long int createFddGroup(fddBase * ref, std::vector<fddBase*> & fdds);

			unsigned long int readFDD(fddBase * ref, const char * fileName);
			void getFDDInfo(size_t & size, std::vector<size_t> & dataAlloc);

			int numProcs(){ return comm->numProcs; }
			

			unsigned long int enqueueTask(fddOpType opT, unsigned long int idSrc, unsigned long int idRes, int funcId, size_t size);
			unsigned long int enqueueTask(fddOpType opT, unsigned long int id, size_t size);

			//void * recvTaskResult(unsigned long int &tid, unsigned long int &sid, size_t & size);
			std::vector< std::pair<void *, size_t> > recvTaskResult(unsigned long int &tid, unsigned long int &sid, system_clock::time_point & start);

			template <typename K>
			void sendKeyMap(unsigned long id, std::unordered_map<K, int> & keyMap);
			template <typename K>
			void sendCogroupData(unsigned long id, std::unordered_map<K, int> & keyMap, std::vector<bool> & flags);
					

			template <typename Z, typename FDD> 
			void collectFDD(Z & ret, FDD * fddP){
				//std::cerr << "    S:SendCollect " ;
				
				comm->sendCollect(fddP->getId());
				//std::cerr << "ID:" << fddP->getId() << " ";

				comm->recvFDDDataCollect(ret);
				
				//std::cerr << ".\n";
			}


			// Propagate FDD data to other machines
			// Primitive types
			template <typename T>
			void parallelize(unsigned long int id, T * data, size_t size){
				size_t offset = 0;
				//std::cerr << "  Parallelize Data\n";

				for (int i = 1; i < comm->numProcs; ++i){
					int dataPerProc = size/(comm->numProcs - 1);
					int rem = size % (comm->numProcs -1);
					if (i <= rem)
						dataPerProc += 1;
					//std::cerr << "    S:FDDSetData P" << i << " ID:" << id << " S:" << dataPerProc << "";

					comm->sendFDDSetData(id, i, &data[offset], dataPerProc);
					offset += dataPerProc;
					//std::cerr << ".\n";
				}
				comm->waitForReq(comm->numProcs - 1);
				//std::cerr << "  Done\n";
			}
			template <typename K, typename T>
			void parallelizeI(unsigned long int id, K * keys, T * data, size_t size){
				size_t offset = 0;
				//std::cerr << "  Parallelize Data\n";

				for (int i = 1; i < comm->numProcs; ++i){
					int dataPerProc = size/(comm->numProcs - 1);
					int rem = size % (comm->numProcs -1);
					if (i <= rem)
						dataPerProc += 1;
					//std::cerr << "    S:FDDSetDataI P" << i << " ID:" << id << " S:" << dataPerProc << "";

					comm->sendFDDSetIData(id, i, &keys[offset], &data[offset], dataPerProc);
					offset += dataPerProc;
					//std::cerr << ".\n";
				}
				comm->waitForReq(comm->numProcs - 1);
				//std::cerr << "  Done\n";
			}

			//Pointers
			template <typename T>
			void parallelize(unsigned long int id, T ** data, size_t * dataSizes, size_t size){
				size_t offset = 0;

				//std::cerr << "  Parallelize Data\n";
				for (int i = 1; i < comm->numProcs; ++i){
					int dataPerProc = size/ (comm->numProcs - 1);
					int rem = size % (comm->numProcs -1);
					if (i <= rem)
						dataPerProc += 1;
					//std::cerr << "    S:FDDSetPData P" << i << " ID:" << id << " S:" << dataPerProc << "";

					comm->sendFDDSetData(id, i, &data[offset], &dataSizes[offset], dataPerProc);
					offset += dataPerProc;
					//std::cerr << ".\n";
				}
				comm->waitForReq(comm->numProcs - 1);
				//std::cerr << "  Done\n";
			}
			template <typename K, typename T>
			void parallelizeI(unsigned long int id, K * keys, T ** data, size_t * dataSizes, size_t size){
				size_t offset = 0;

				//std::cerr << "  Parallelize Data\n";
				for (int i = 1; i < comm->numProcs; ++i){
					int dataPerProc = size/ (comm->numProcs - 1);
					int rem = size % (comm->numProcs -1);
					if (i <= rem)
						dataPerProc += 1;
					//std::cerr << "    S:FDDSetPDataI P" << i << " ID:" << id << " S:" << dataPerProc << "";

					comm->sendFDDSetIData(id, i, &keys[offset],  &data[offset], &dataSizes[offset], dataPerProc);
					offset += dataPerProc;
					//std::cerr << ".\n";
				}
				comm->waitForReq(comm->numProcs - 1);
				//std::cerr << "  Done\n";
			}


			std::vector<size_t> getAllocation(size_t size);

			void discardFDD(unsigned long int id);

	};

	template <class T>
	void fastContext::registerGlobal(T * varP){
		std::pair<void*, size_t> globalReg ;
		globalReg.first = varP;
		globalReg.second = sizeof(T);
		globalTable.insert( globalTable.end(), globalReg );
	}
	
	template <typename K>
	void fastContext::sendKeyMap(unsigned long tid, std::unordered_map<K, int> & keyMap){
		comm->sendKeyMap(tid, keyMap);
	}
	template <typename K>
	void fastContext::sendCogroupData(unsigned long tid, std::unordered_map<K, int> & keyMap, std::vector<bool>  &flags){
		comm->sendCogroupData(tid, keyMap, flags);
	}


}

#endif
