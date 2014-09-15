#ifndef LIBFASTER_GROUPEDFDD_H
#define LIBFASTER_GROUPEDFDD_H


//#include "indexedFdd.h"
#include "fastContext.h"

namespace faster{

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
			}
			fddBase * _map (void * funcP, fddOpType op, fddBase * newFdd);
			template <typename To> 
			fdd<To> * map(void * funcP, fddOpType op);
			template <typename Ko, typename To> 
			indexedFdd<Ko,To> * mapI(void * funcP, fddOpType op);

			groupedFdd<K> * update(void * funcP, fddOpType op);

			void cogroup(std::unordered_map<K, int> & keyMap);
		public:
			template <typename T, typename U> 
			groupedFdd(fastContext * c, iFddCore<K,T> * fdd0, iFddCore<K,U> * fdd1, std::unordered_map<K, int> & keyMap) : groupedFdd(c) { 
				members.insert(members.end(), fdd0);
				members.insert(members.end(), fdd1);
				id = context->createFddGroup(this, members); 
				cogroup(keyMap);
			}
			template <typename T, typename U, typename V> 
			groupedFdd(fastContext * c, iFddCore<K,T> * fdd0, iFddCore<K,U> * fdd1,  iFddCore<K,V> * fdd2, std::unordered_map<K, int> & keyMap) : groupedFdd(c){
				members.insert(members.end(), fdd0);
				members.insert(members.end(), fdd1);
				members.insert(members.end(), fdd2);
				id = context->createFddGroup(this, members); 
				cogroup(keyMap);
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


			// MapByKey
			template <typename Ko, typename To> 
			indexedFdd<Ko,To> * mapByKey( ImapByKeyG2FunctionP<K,Ko,To> funcP){
				return (indexedFdd<Ko,To> *) mapI<Ko,To>((void*) funcP, OP_MapByKey);
			}

			template <typename V, typename Ko, typename To> 
			indexedFdd<Ko,To> * mapByKey( ImapByKeyG3FunctionP<K,Ko,To> funcP){
				return (indexedFdd<Ko,To> *) mapI<Ko,To>((void*) funcP, OP_MapByKey);
			}
			template <typename To> 
			fdd<To> * mapByKey( mapByKeyG2FunctionP<K,To> funcP){
				return (fdd<To> *) map<To>((void*) funcP, OP_MapByKey);
			}

			template <typename V, typename To> 
			fdd<To> * mapByKey( mapByKeyG3FunctionP<K,To> funcP){
				return (fdd<To> *) map<To>((void*) funcP, OP_MapByKey);
			}

			// FlatMapByKey

			template <typename Ko, typename To> 
			indexedFdd<Ko,To> * flatMapByKey( IflatMapByKeyG2FunctionP<K,Ko,To> funcP){
				return (indexedFdd<Ko,To> *) mapI<Ko,To>((void*) funcP, OP_FlatMapByKey);
			}

			template <typename V, typename Ko, typename To> 
			indexedFdd<Ko,To> * flatMapByKey( IflatMapByKeyG3FunctionP<K,Ko,To> funcP){
				return (indexedFdd<Ko,To> *) mapI<Ko,To>((void*) funcP, OP_FlatMapByKey);
			}
			template <typename To> 
			fdd<To> * flatMapByKey( flatMapByKeyG2FunctionP<K,To> funcP){
				return (fdd<To> *) map<To>((void*) funcP, OP_FlatMapByKey);
			}

			template <typename V, typename To> 
			fdd<To> * flatMapByKey( flatMapByKeyG3FunctionP<K,To> funcP){
				return (fdd<To> *) map<To>((void*) funcP, OP_FlatMapByKey);
			}

			void discard(){
				for ( int i = 0; i < members.size(); ++i){
					members[i]->discard();
				}
			}
			void setKeyMap(void * keyMap UNUSED) {}
			void setGroupedByKey(bool gbk UNUSED) {}
		
	};

	template <typename K>
	fddBase * groupedFdd<K>::_map (void * funcP, fddOpType op, fddBase * newFdd){
		std::cerr << "  Map\n";
		size_t result;
		size_t rSize;
		size_t fddSize;
		unsigned long int tid, sid;
		unsigned long int newFddId = newFdd->getId();

		// Decode function pointer
		int funcId = context->findFunc(funcP);

		// Send task
		context->enqueueTask(op, id, newFddId, funcId, this->size);

		// Receive results
		fddSize = 0;
		for (int i = 1; i < context->numProcs(); ++i){
			result = * (size_t*) context->recvTaskResult(tid, sid, rSize);
			if ( (op & 0xff) & (OP_MapByKey | OP_FlatMapByKey | OP_FlatMap) )
				fddSize += result;
		}
		if ( (op & 0xff) & (OP_MapByKey | OP_FlatMapByKey | OP_FlatMap) )
			newFdd->setSize(fddSize);

		for ( int i = 0; i < members.size(); ++i){
			if (!members[i]->isCached()){
				members[i]->discard();
			}
		}

		std::cerr << "  Done\n";
		return newFdd;
	}
	template <typename K>
	template <typename To> 
	fdd<To> * groupedFdd<K>::map (void * funcP, fddOpType op){
		fdd<To> * newFdd;

		if ( (op & 0xFF ) & (OP_MapByKey | OP_FlatMapByKey | OP_FlatMap | OP_BulkFlatMap) ){
			newFdd = new fdd<To>(*context);
		}else{
			newFdd = new fdd<To>(*context, size);
		}

		return (fdd<To> *) _map(funcP, op, newFdd);
	}
	template <typename K>
	template <typename Ko, typename To> 
	indexedFdd<Ko,To> * groupedFdd<K>::mapI(void * funcP, fddOpType op){
		indexedFdd<Ko,To> * newFdd;

		if ( (op & 0xFF ) & (OP_MapByKey | OP_FlatMapByKey | OP_FlatMap | OP_BulkFlatMap) ){
			newFdd = new indexedFdd<Ko,To>(*context);
		}else{
			newFdd = new indexedFdd<Ko,To>(*context, size);
		}

		return (indexedFdd<Ko,To> *) _map(funcP, op, newFdd);
	}

	template <typename K>
	groupedFdd<K> * groupedFdd<K>::update(void * funcP, fddOpType op){

		// Decode function pointer
		int funcId = context->findFunc(funcP);

		// Send task
		unsigned long int tid = context->enqueueTask(op, this->id, 0, funcId, this->size);

		return this;
	}

	template <typename K>
	void groupedFdd<K>::cogroup(std::unordered_map<K, int> & keyMap){
		unsigned long int tid = context->enqueueTask(OP_CoGroup, id, this->size);
		void * result;
		size_t rSize;
		unsigned long int sid;

		context->sendKeyMap(tid, keyMap);

		for (size_t i = 1; i < members.size(); ++i){
			members[i]->setKeyMap(&keyMap);
			members[i]->setGroupedByKey(true);
		}

		for (size_t i = 1; i < context->numProcs(); ++i){
			result = context->recvTaskResult(tid, sid, rSize);
		}
	}


}

#endif
