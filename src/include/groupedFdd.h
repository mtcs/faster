#ifndef LIBFASTER_GROUPEDFDD_H
#define LIBFASTER_GROUPEDFDD_H

#include <memory>

//#include "indexedFdd.h"
#include "misc.h"
#include "fastContext.h"

namespace faster{

	template <class K, class T> 
	class iFddCore ; 

	template <class K, class T> 
	class indexedFdd ; 

	/*template<typename K, typename... Types> 
	class groupedFdd : fddBase{
		private:
			static const unsigned short int dimension = sizeof...(Types);
			fastContext * context;
			std::vector<fddBase *> members;

			template <typename T, typename... iFdds>
			void addFdds(iFddCore<K,T> * fdd, iFdds ... otherFdds){
				members.insert(members.end(), fdd);

				addFdds(otherFdds... );
			}

			template <typename T, typename... OtherArgs, typename... OtherSizes, typename U>
			void unpackMap( std::pair<K,U> (*funcP) (const K &, T *, OtherArgs ... otherArgs, size_t, OtherSizes ... otherSizes), fddOpType op){
				unpackMap<K, OtherArgs..., U, OtherSizes...>(funcP, op);
			}

		public:
			template <typename... FddTypes> 
			groupedFdd(fastContext * c, FddTypes * ... fddTypes){
				context = c;

				addFdds(fddTypes...);
				//context->coPartition(members);
				id = context->createFddGroup(this, members); 
			}

			template <typename... Sizes, typename U> 
			indexedFdd<K,U> * mapByKey( std::pair<K,U> (*funcP) (const K & key, Types * ... args, Sizes ... sizes) ){
				unpackMap<K,U>(funcP, OP_Map);
				return NULL; //map<K,U>((void*) funcP, OP_Map);
			}

	};*/
	template<typename K> 
	class groupedFdd : fddBase{
		private:
			int numMembers;
			fastContext * context;
			std::vector<fddBase *> members;

			groupedFdd(fastContext * c){
					_kType = decodeType(typeid(K).hash_code());
					_tType = Null;
					context = c;
					members.reserve(4);
					cached = false;
			}
			fddBase * _map (void * funcP, fddOpType op, fddBase * newFdd, system_clock::time_point & start);
			template <typename To> 
			fdd<To> * map(void * funcP, fddOpType op);
			template <typename Ko, typename To> 
			indexedFdd<Ko,To> * mapI(void * funcP, fddOpType op);

			groupedFdd<K> * update(void * funcP, fddOpType op);

			void cogroup(std::shared_ptr<std::unordered_map<K, int>> & keyMap, system_clock::time_point & start);
		public:
			template <typename T, typename U> 
			groupedFdd(fastContext * c, iFddCore<K,T> * fdd0, iFddCore<K,U> * fdd1, std::shared_ptr<std::unordered_map<K, int>> & keyMap, system_clock::time_point & start) : groupedFdd(c) { 
				members.insert(members.end(), fdd0);
				members.insert(members.end(), fdd1);
				id = context->createFddGroup(this, members); 
				cogroup(keyMap, start);
			}
			template <typename T, typename U, typename V> 
			groupedFdd(fastContext * c, iFddCore<K,T> * fdd0, iFddCore<K,U> * fdd1,  iFddCore<K,V> * fdd2, std::shared_ptr<std::unordered_map<K, int>> & keyMap, system_clock::time_point & start) : groupedFdd(c){
				members.insert(members.end(), fdd0);
				members.insert(members.end(), fdd1);
				members.insert(members.end(), fdd2);
				id = context->createFddGroup(this, members); 
				cogroup(keyMap, start);
			}

			groupedFdd<K> * cache(){
				this->cached = true;
				return this;
			}

			// UpdateByKey
			groupedFdd<K> * updateByKey( updateByKeyG2FunctionP<K> funcP){
				update((void*) funcP, OP_UpdateByKey);
				return this;
			}

			groupedFdd<K> * updateByKey( updateByKeyG3FunctionP<K> funcP){
				update((void*) funcP, OP_UpdateByKey);
				return this;
			}

			// BulkUpdateByKey
			groupedFdd<K> * bulkUpdate( bulkUpdateG2FunctionP<K> funcP){
				update((void*) funcP, OP_BulkUpdate);
				return this;
			}

			groupedFdd<K> * bulkUpdate( bulkUpdateG3FunctionP<K> funcP){
				update((void*) funcP, OP_BulkUpdate);
				return this;
			}


			// MapByKey
			template <typename Ko, typename To> 
			indexedFdd<Ko,To> * mapByKey( ImapByKeyG2FunctionP<K,Ko,To> funcP){
				return (indexedFdd<Ko,To> *) mapI<Ko,To>((void*) funcP, OP_MapByKey);
			}

			template <typename Ko, typename To> 
			indexedFdd<Ko,To> * mapByKey( ImapByKeyG3FunctionP<K,Ko,To> funcP){
				return (indexedFdd<Ko,To> *) mapI<Ko,To>((void*) funcP, OP_MapByKey);
			}
			template <typename To> 
			fdd<To> * mapByKey( mapByKeyG2FunctionP<K,To> funcP){
				return (fdd<To> *) map<To>((void*) funcP, OP_MapByKey);
			}

			template <typename To> 
			fdd<To> * mapByKey( mapByKeyG3FunctionP<K,To> funcP){
				return (fdd<To> *) map<To>((void*) funcP, OP_MapByKey);
			}

			// FlatMapByKey

			template <typename Ko, typename To> 
			indexedFdd<Ko,To> * flatMapByKey( IflatMapByKeyG2FunctionP<K,Ko,To> funcP){
				return (indexedFdd<Ko,To> *) mapI<Ko,To>((void*) funcP, OP_FlatMapByKey);
			}

			template <typename Ko, typename To> 
			indexedFdd<Ko,To> * flatMapByKey( IflatMapByKeyG3FunctionP<K,Ko,To> funcP){
				return (indexedFdd<Ko,To> *) mapI<Ko,To>((void*) funcP, OP_FlatMapByKey);
			}
			template <typename To> 
			fdd<To> * flatMapByKey( flatMapByKeyG2FunctionP<K,To> funcP){
				return (fdd<To> *) map<To>((void*) funcP, OP_FlatMapByKey);
			}

			template <typename To> 
			fdd<To> * flatMapByKey( flatMapByKeyG3FunctionP<K,To> funcP){
				return (fdd<To> *) map<To>((void*) funcP, OP_FlatMapByKey);
			}

			void discard(){
				context->discardFDD(id);
				dataAlloc.clear();
			}


			// Bulk Flat Map
			template <typename Ko, typename To> 
			indexedFdd<Ko,To> * bulkFlatMap( IbulkFlatMapG2FunctionP<K, Ko,To> funcP ){
				return mapI<Ko,To>((void*) funcP, OP_BulkFlatMap);
			}
			template <typename Ko, typename To> 
			indexedFdd<Ko,To> * bulkFlatMap( IbulkFlatMapG3FunctionP<K, Ko,To> funcP ){
				return mapI<Ko,To>((void*) funcP, OP_BulkFlatMap);
			}
			template <typename To> 
			fdd<To> * bulkFlatMap( bulkFlatMapG2FunctionP<K, To> funcP ){
				return map<To>((void*) funcP, OP_BulkFlatMap);
			}
			template <typename To> 
			fdd<To> * bulkFlatMap( bulkFlatMapG3FunctionP<K, To> funcP ){
				return map<To>((void*) funcP, OP_BulkFlatMap);
			}


			void * getKeyMap() { return NULL; }
			void setKeyMap(void * keyMap UNUSED) {}
			bool isGroupedByKey() { return false; }
			void setGroupedByKey(bool gbk UNUSED) {}
		
	};

	template <typename K>
	fddBase * groupedFdd<K>::_map (void * funcP, fddOpType op, fddBase * newFdd, system_clock::time_point & start){
		//std::cerr << "  Map ";
		unsigned long int tid, sid;
		unsigned long int newFddId = newFdd->getId();

		// Decode function pointer
		int funcId = context->findFunc(funcP);

		// Send task
		context->enqueueTask(op, id, newFddId, funcId, this->size);

		// Receive results
		auto result = context->recvTaskResult(tid, sid, start);
			
		if ( (op & 0xff) & (OP_MapByKey | OP_FlatMapByKey | OP_FlatMap | OP_BulkFlatMap) ){
			size_t fddSize = 0;

			for (int i = 1; i < context->numProcs(); ++i){
				if (result[i].second > 0) fddSize += * (size_t*) result[i].first;
			}
			newFdd->setSize(fddSize);
		}

		if ( ! cached ){
			for ( size_t i = 0; i < members.size(); ++i){
				if ( ! members[i]->isCached() ){
					members[i]->discard();
					//std::cerr << "GD" << id << " "; 
				}
			}
			discard();
		}

		//std::cerr << "\n";
		return newFdd;
	}
	template <typename K>
	template <typename To> 
	fdd<To> * groupedFdd<K>::map (void * funcP, fddOpType op){
		fdd<To> * newFdd = NULL;
		auto start = system_clock::now();

		if ( (op & 0xFF ) & (OP_MapByKey | OP_FlatMapByKey | OP_FlatMap | OP_BulkFlatMap) ){
			newFdd = new fdd<To>(*context);
		}else{
			newFdd = new fdd<To>(*context, size);
		}

		return (fdd<To> *) _map(funcP, op, newFdd, start);
	}
	template <typename K>
	template <typename Ko, typename To> 
	indexedFdd<Ko,To> * groupedFdd<K>::mapI(void * funcP, fddOpType op){
		indexedFdd<Ko,To> * newFdd = NULL;
		auto start = system_clock::now();

		if ( (op & 0xFF ) & (OP_MapByKey | OP_FlatMapByKey | OP_FlatMap | OP_BulkFlatMap) ){
			newFdd = new indexedFdd<Ko,To>(*context);
		}else{
			newFdd = new indexedFdd<Ko,To>(*context, size);
		}

		return (indexedFdd<Ko,To> *) _map(funcP, op, newFdd, start);
	}

	template <typename K>
	groupedFdd<K> * groupedFdd<K>::update(void * funcP, fddOpType op){
		auto start = system_clock::now();
		unsigned long int sid;

		// Decode function pointer
		int funcId = context->findFunc(funcP);

		// Send task
		unsigned long int tid = context->enqueueTask(op, this->id, 0, funcId, this->size);

		auto result = context->recvTaskResult(tid, sid, start);

		if ( ! cached ){
			for ( size_t i = 0; i < members.size(); ++i){
				if ( ! members[i]->isCached() ){
					members[i]->discard();
				}
			}
			discard();
		}

		return this;
	}

	template <typename K>
	void groupedFdd<K>::cogroup(std::shared_ptr<std::unordered_map<K, int>> & keyMap, system_clock::time_point & start){
		using std::chrono::system_clock;
		std::vector<bool> exchangeData (members.size()-1, true);

		unsigned long int sid;

		for (size_t i = 1; i < members.size(); ++i){
			if ( members[i]->isGroupedByKey() ){
				void * km = members[i]->getKeyMap();
				if ( *(std::shared_ptr<std::unordered_map<K, int>>*)km != keyMap ){
					members[i]->setKeyMap(&keyMap);
				}else{
					exchangeData[i-1] = false;
				}
			}else{
				members[i]->setKeyMap(&keyMap);
				members[i]->setGroupedByKey(true);
			}
		}

		unsigned long int tid = context->enqueueTask(OP_CoGroup, id, this->size);

		context->sendCogroupData(tid, *keyMap, exchangeData);


		auto result = context->recvTaskResult(tid, sid, start);
	}


}

#endif
