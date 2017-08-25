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
#include <memory>

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


	/// @brief Context Configuration Class.
	///
	/// Throught the fastSetting Class,
	/// the programmer can change default framework settings.
	/// like ...
	class fastSettings{
		friend class fastContext;
		public:

			/// @brief fastSetting default constructor
			fastSettings() {
				_allowDataBalancing = false;
			}

			/// @brief fastSetting dummy constructor
			fastSettings(const fastSettings & s UNUSED){
			}

			/// @brief Enables dynamic load balancing
			void allowDataBalancing(){
				_allowDataBalancing = true;
			}

		private:
			bool _allowDataBalancing;

	};

	/// @brief Framework context class.
	///
	/// The context manages communication, scheduler and start Workers.
	/// A context is needed to create datasets!
	///
	class fastContext{

		template <class T> friend class fdd;
		template <class T> friend class fddCore;
		template <class K, class T> friend class iFddCore;
		template <class K, class T> friend class indexedFdd;
		template <class K> friend class groupedFdd;
		friend class worker;

		public:
			/// @brief fastContext default constructor
			///
			/// @param argc - number of arguments from main
			/// @param argv - arguments from main
			fastContext( int argc=0, char ** argv=NULL);

			/// fastContext constructor with custom settings
			fastContext( const fastSettings & s, int argc, char ** argv);

			/// fastContext destructor
			~fastContext();

			/// @name Function and global variables registration
			/// @{

			/// @brief Register a user custom function in the context.
			///
			/// Registering a user custom functions is necessary in order to pass it as
			/// parametes to FDD functions like __map__ and __reduce__.
			///
			/// @param funcP - Function pointer to a user defined function.
			void registerFunction(void * funcP);

			/// @brief Register a user custom function in the context.
			///
			/// Registering a user custom functions is necessary in order to pass it as
			/// parametes to FDD functions like __map__ and __reduce__.
			///
			/// @param funcP - Function pointer to a user defined function.
			/// @param name - Custom name to registered funciton.
			void registerFunction(void * funcP, const std::string name);


			/// @brief Gegisters a primitive global varible to be used inside used defined
			///        functions in distributted environment.
			///
			/// @tparam T - Type of the global variable to be registered
			/// @param varP - Global variable to be registered
			template <class T>
			void registerGlobal(T * varP);

			/// @brief Gegisters a global array to be used inside used defined
			///        functions in distributted environment.
			///
			/// @tparam T - Type of the global array to be registered
			/// @param varP - Global array to be registered
			/// @param s - Size of the array
			template <class T>
			void registerGlobal(T ** varP, size_t s);

			/// @brief Gegisters a global Vector to be used inside used defined
			///        functions in distributted environment.
			///
			/// @tparam T - Type of the global vector to be registered
			/// @param varP - Global vector to be registered
			template <class T>
			void registerGlobal(std::vector<T> * varP);
			/// @}

			/// @brief Start worker machines computation
			///
			/// When this function is called, the driver processes and works processes
			/// diverge from execution. While the Driver process starts to execute user
			/// code, the worker processes start to waiting for tasks. Then workers
			/// should exit short after this function is called.
			void startWorkers();

			/// @brief Checks for the driver process
			///
			/// @returns  -  true if the process is the driver process
			bool isDriver();
			/// @brief Return the number of processes running
			///
			/// @return number of active processes
			int numProcs(){ return comm->numProcs; }


			/// @brief Performs a microbenchmark to do dynamic load balancing (UNUSED)
			void calibrate();

			/// @name Online file reading and parsing
			/// @{

			/// @brief Reads a file with online parsing and partition (NOT IMPLEMENTED)
			///
			/// @tparam T - Dataset type
			/// @param path - Input file path
			/// @param funcP - partition function pointer of types ::faster::onlineFullPartFuncP or ::faster::IonlineFullPartFuncP
			///
			/// @returns - a dataset of ::faster::fdd<t> type and faster::indexedFdd<K,T>
			template <typename T>
			fdd<T> * onlineFullPartRead(std::string path, onlineFullPartFuncP<T> funcP);
			template <typename K, typename T>
			indexedFdd<K,T> * onlineFullPartRead(std::string path, IonlineFullPartFuncP<K,T> funcP);
			template <typename K, typename T>
			indexedFdd<K,T> * onlinePartRead(std::string path, IonlineFullPartFuncP<K,T> funcP);

			/// @brief Reads a file with online parsing and mapping (?)
			///
			/// @tparam K - Dataset key type
			/// @tparam T - Dataset type
			/// @param path - File path
			/// @param funcP - (?)
			///
			/// @return
			template <typename T>
			fdd<T> * onlineRead(std::string path, onlineFullPartFuncP<T> funcP);
			template <typename K, typename T>
			indexedFdd<K,T> * onlineRead(std::string path, IonlineFullPartFuncP<K,T> funcP);
			/// @}

			/// @name Task execution profiling
			/// @{

			/// @brief Prints task execution information for all tasks executed by the user.
			void printInfo();
			/// @brief Prints a header for task execution information.
			///
			/// To be used with faster::fastContext::updateInfo()
			void printHeader();
			/// @brief Prints information from tesk ran since last faster::fastContext::updateInfo() called.
			///
			/// To be used with faster::fastContext::printHeader()
			void updateInfo();
			/// @}

		private:
			int id;
			unsigned long int numFDDs;
			//unsigned long int numTasks;
			fastSettings * settings;
			std::vector< fddBase * > fddList;
			std::vector< bool > fddInternal;
			std::vector< void * > funcTable;
			std::vector<std::string> funcName;
			std::vector< std::tuple<void*, size_t, int> > globalTable;
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
			void writeToFile(unsigned long int id,std::string & path, std::string & sufix);


			unsigned long int enqueueTask(fddOpType opT, unsigned long int idSrc, unsigned long int idRes, int funcId, size_t size);
			unsigned long int enqueueTask(fddOpType opT, unsigned long int id, size_t size);

			//void * recvTaskResult(unsigned long int &tid, unsigned long int &sid, size_t & size);
			std::vector< std::pair<void *, size_t> > recvTaskResult(unsigned long int &tid, unsigned long int &sid, system_clock::time_point & start);

			template <typename K>
			void sendKeyMap(unsigned long id, std::unordered_map<K, int> & keyMap);
			template <typename K>
			void sendCogroupData(unsigned long id, std::unordered_map<K, int> & keyMap, std::vector<bool> & flags);

			void setInternal(size_t i, bool b){
				fddInternal[i] = b;
			}

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
		// TODO Adapt to new global model
		globalTable.insert( globalTable.end(), std::make_tuple(varP, sizeof(T), 0) );
	}
	template <class T>
	void fastContext::registerGlobal(T ** varP, size_t s){
		// TODO Adapt to new global model
		globalTable.insert( globalTable.end(), std::make_tuple(varP, s, POINTER) );
	}
	// Still unimplemented
	template <class T>
	void fastContext::registerGlobal(std::vector<T> * varP){
		// TODO Adapt to new global model
		globalTable.insert( globalTable.end(), std::make_tuple(varP->data(), sizeof(T) * varP->size(), VECTOR) );
	}

	/*template <typename T>
	fdd<T> * fastContext::onlineFullPartRead(std::string path, onlineFullPartFuncP<T> funcP){
		auto start = system_clock::now();
		fdd<T> * newFdd = new fdd<T>(*this);
		int funcId = findFunc((void*)funcP);
		std::vector<size_t> alloc(comm->numProcs, 0);
		unsigned long int sid;
		size_t newSize = 0;

		// Send task
		unsigned long int tid = enqueueTask(OP_OnFullPRead, newFdd->getId(), -1, funcId, 0);
		for (int i = 1; i < numProcs(); ++i){
			comm->sendFileName(path, i, 0, 0);
		}

		// Receive results
		auto result = recvTaskResult(tid, sid, start);

		for (int i = 1; i < numProcs(); ++i){
			if (result[i].second > 0){
				alloc[i] = * (size_t*) result[i].first;
				newSize += alloc[i];
			}
		}
		newFdd->setSize(newSize);
		scheduler->setAllocation(alloc, newSize);

		fddList.insert(fddList.begin(), newFdd);

		return newFdd;
	}*/

	template <typename K, typename T>
	indexedFdd<K,T> * fastContext::onlineFullPartRead(std::string path, IonlineFullPartFuncP<K,T> funcP){
		auto start = system_clock::now();
		indexedFdd<K,T> * newFdd = new indexedFdd<K,T>(*this);
		int funcId = findFunc((void*)funcP);
		std::vector<size_t> alloc(comm->numProcs, 0);
		unsigned long int sid;
		size_t newSize = 0;

		// Send task
		unsigned long int tid = enqueueTask(OP_OnFullPRead, newFdd->getId(), -1, funcId, 0);
		comm->sendFileName(path);

		// Receive results
		auto result = recvTaskResult(tid, sid, start);

		for (int i = 1; i < numProcs(); ++i){
			if (result[i].second > 0){
				alloc[i] = * (size_t*) result[i].first;
				newSize += alloc[i];
			}
		}
		newFdd->setSize(newSize);
		newFdd->setGroupedByKey(true);
		newFdd->setGroupedByMap(true);
		scheduler->setAllocation(alloc, newSize);

		return newFdd;
	}


	template <typename K, typename T>
	indexedFdd<K,T> * fastContext::onlinePartRead(std::string path, IonlineFullPartFuncP<K,T> funcP){
		auto start = system_clock::now();
		indexedFdd<K,T> * newFdd = new indexedFdd<K,T>(*this);
		int funcId = findFunc((void*)funcP);
		std::vector<size_t> alloc(comm->numProcs, 0);
		unsigned long int sid;
		size_t newSize = 0;

		// Send task
		unsigned long int tid = enqueueTask(OP_OnPartRead, newFdd->getId(), -1, funcId, 0);
		comm->sendFileName(path);

		// Receive results
		auto result = recvTaskResult(tid, sid, start);

		for (int i = 1; i < numProcs(); ++i){
			if (result[i].second > 0){
				alloc[i] = * (size_t*) result[i].first;
				newSize += alloc[i];
			}
		}
		newFdd->setSize(newSize);
		newFdd->setGroupedByKey(true);
		newFdd->setGroupedByMap(true);
		scheduler->setAllocation(alloc, newSize);

		return newFdd;
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
