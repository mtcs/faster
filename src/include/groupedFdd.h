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

			void cogroup(system_clock::time_point & start);
		public:
			/// @brief Creates a indexedFdd group with two members
			///
			/// @tparam T - value type of the first dataset
			/// @tparam U - value type of the second dataset
			/// @param c - the context
			/// @param fdd0 - first dataset
			/// @param fdd1 - second dataset
			/// @param start - start timestamp
			template <typename T, typename U>
			groupedFdd(fastContext * c, iFddCore<K,T> * fdd0, iFddCore<K,U> * fdd1, system_clock::time_point & start) : groupedFdd(c) {
				members.insert(members.end(), fdd0);
				members.insert(members.end(), fdd1);
				id = context->createFddGroup(this, members);
				cogroup(start);
			}
			/// @brief Creates a indexedFdd group with two members
			///
			/// @tparam T - value type of the first dataset
			/// @tparam U - value type of the second dataset
			/// @tparam V - value type of the third dataset
			/// @param c - the context
			/// @param fdd0 - first dataset
			/// @param fdd1 - second dataset
			/// @param fdd2 - third dataset
			/// @param start - start timestamp
			template <typename T, typename U, typename V>
			groupedFdd(fastContext * c, iFddCore<K,T> * fdd0, iFddCore<K,U> * fdd1,  iFddCore<K,V> * fdd2, system_clock::time_point & start) : groupedFdd(c){
				members.insert(members.end(), fdd0);
				members.insert(members.end(), fdd1);
				members.insert(members.end(), fdd2);
				id = context->createFddGroup(this, members);
				cogroup(start);
			}

			/// @addtogroup memmodel
			/// @{

			/// @brief Prevents automatic memory deallocation from hapenning
			///
			/// @return pointer to the cached dataset (self)
			groupedFdd<K> * cache(){
				this->cached = true;
				return this;
			}

			/// @brief deallocates previously cached fdd
			void discard(){
				context->discardFDD(id);
				dataAlloc.clear();
			}
			/// @}

			bool isGroupedByKey() { return false; }
			void setGroupedByKey(bool gbk UNUSED) {}

			/// @addtogroup update
			/// @{

			// UpdateByKey
			/// @ingroup grouped
			groupedFdd<K> * updateByKey( updateByKeyG2FunctionP<K> funcP){
				update((void*) funcP, OP_UpdateByKey);
				return this;
			}

			/// @ingroup grouped
			groupedFdd<K> * updateByKey( updateByKeyG3FunctionP<K> funcP){
				update((void*) funcP, OP_UpdateByKey);
				return this;
			}

			/// @ingroup bulk
			/// @ingroup grouped
			// BulkUpdateByKey
			groupedFdd<K> * bulkUpdate( bulkUpdateG2FunctionP<K> funcP){
				update((void*) funcP, OP_BulkUpdate);
				return this;
			}

			/// @ingroup bulk
			/// @ingroup grouped
			groupedFdd<K> * bulkUpdate( bulkUpdateG3FunctionP<K> funcP){
				update((void*) funcP, OP_BulkUpdate);
				return this;
			}
			/// @}


			// MapByKey
			/// @addtogroup map
			/// @{

			/// @ingroup bykey
			/// @ingroup grouped
			template <typename Ko, typename To>
			indexedFdd<Ko,To> * mapByKey( ImapByKeyG2FunctionP<K,Ko,To> funcP){
				return (indexedFdd<Ko,To> *) mapI<Ko,To>((void*) funcP, OP_MapByKey);
			}

			/// @ingroup bykey
			/// @ingroup grouped
			template <typename Ko, typename To>
			indexedFdd<Ko,To> * mapByKey( ImapByKeyG3FunctionP<K,Ko,To> funcP){
				return (indexedFdd<Ko,To> *) mapI<Ko,To>((void*) funcP, OP_MapByKey);
			}

			/// @ingroup bykey
			/// @ingroup grouped
			template <typename To>
			fdd<To> * mapByKey( mapByKeyG2FunctionP<K,To> funcP){
				return (fdd<To> *) map<To>((void*) funcP, OP_MapByKey);
			}

			/// @ingroup bykey
			template <typename To>
			fdd<To> * mapByKey( mapByKeyG3FunctionP<K,To> funcP){
				return (fdd<To> *) map<To>((void*) funcP, OP_MapByKey);
			}
			/// @}

			// FlatMapByKey
			/// @addtogroup flatmap
			/// @{

			/// @ingroup bykey
			/// @ingroup grouped
			template <typename Ko, typename To>
			indexedFdd<Ko,To> * flatMapByKey( IflatMapByKeyG2FunctionP<K,Ko,To> funcP){
				return (indexedFdd<Ko,To> *) mapI<Ko,To>((void*) funcP, OP_FlatMapByKey);
			}

			/// @ingroup bykey
			/// @ingroup grouped
			template <typename Ko, typename To>
			indexedFdd<Ko,To> * flatMapByKey( IflatMapByKeyG3FunctionP<K,Ko,To> funcP){
				return (indexedFdd<Ko,To> *) mapI<Ko,To>((void*) funcP, OP_FlatMapByKey);
			}

			/// @ingroup bykey
			/// @ingroup grouped
			template <typename To>
			fdd<To> * flatMapByKey( flatMapByKeyG2FunctionP<K,To> funcP){
				return (fdd<To> *) map<To>((void*) funcP, OP_FlatMapByKey);
			}

			/// @ingroup bykey
			/// @ingroup grouped
			template <typename To>
			fdd<To> * flatMapByKey( flatMapByKeyG3FunctionP<K,To> funcP){
				return (fdd<To> *) map<To>((void*) funcP, OP_FlatMapByKey);
			}


			// Bulk Flat Map
			/// @ingroup bulk
			/// @ingroup grouped
			template <typename Ko, typename To>
			indexedFdd<Ko,To> * bulkFlatMap( IbulkFlatMapG2FunctionP<K, Ko,To> funcP ){
				return mapI<Ko,To>((void*) funcP, OP_BulkFlatMap);
			}
			/// @ingroup bulk
			/// @ingroup grouped
			template <typename Ko, typename To>
			indexedFdd<Ko,To> * bulkFlatMap( IbulkFlatMapG3FunctionP<K, Ko,To> funcP ){
				return mapI<Ko,To>((void*) funcP, OP_BulkFlatMap);
			}
			/// @ingroup bulk
			/// @ingroup grouped
			template <typename To>
			fdd<To> * bulkFlatMap( bulkFlatMapG2FunctionP<K, To> funcP ){
				return map<To>((void*) funcP, OP_BulkFlatMap);
			}
			/// @ingroup bulk
			/// @ingroup grouped
			template <typename To>
			fdd<To> * bulkFlatMap( bulkFlatMapG3FunctionP<K, To> funcP ){
				return map<To>((void*) funcP, OP_BulkFlatMap);
			}
			/// @}



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
	void groupedFdd<K>::cogroup(system_clock::time_point & start){
		using std::chrono::system_clock;
		using std::chrono::duration_cast;
		using std::chrono::milliseconds;

		//std::cerr << "        DCogroup";
		start = system_clock::now();

		unsigned long int sid;
		//std::cerr << " Init:" << duration_cast<milliseconds>(system_clock::now() - start).count() << "\n";
		//start = system_clock::now();
		unsigned long int tid = context->enqueueTask(OP_CoGroup, id, this->size);

		for (size_t i = 1; i < members.size(); ++i){
			members[i]->setGroupedByKey(true);
		}

		auto result = context->recvTaskResult(tid, sid, start);
		//std::cerr << " Process:" << duration_cast<milliseconds>(system_clock::now() - start).count() << "\n";
	}


}

#endif
