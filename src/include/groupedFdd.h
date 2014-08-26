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
			template <typename Ko, typename To> 
			fddBase * map (void * funcP, fddOpType op);

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

			template <typename T, typename U, typename Ko, typename To> 
			indexedFdd<Ko,To> * mapByKey( ImapByKeyG2FunctionP<K,Ko,To> funcP){
				return (indexedFdd<Ko,To> *) map<Ko,To>((void*) funcP, OP_MapByKey);
			}

			template <typename T, typename U, typename V, typename Ko, typename To> 
			indexedFdd<Ko,To> * mapByKey( ImapByKeyG3FunctionP<K,Ko,To> funcP){
				return (indexedFdd<Ko,To> *) map<Ko,To>((void*) funcP, OP_MapByKey);
			}

	};

	template <typename K>
	template <typename Ko, typename To> 
	fddBase * groupedFdd<K>::map (void * funcP, fddOpType op){
		std::cerr << "  Map\n";
		indexedFdd<Ko,To> * newFdd;
		size_t result;
		size_t rSize;
		size_t fddSize;
		unsigned long int tid, sid;

		if ( (op & 0xFF ) & (OP_MapByKey | OP_FlatMap | OP_BulkFlatMap) ){
			newFdd = new indexedFdd<Ko,To>(*context);
		}else{
			newFdd = new indexedFdd<Ko,To>(*context, size);
		}
		unsigned long int newFddId = newFdd->getId();

		// Decode function pointer
		int funcId = context->findFunc(funcP);

		// Send task
		context->enqueueTask(op, id, newFddId, funcId, this->size);

		// Receive results
		fddSize = 0;
		for (int i = 1; i < context->numProcs(); ++i){
			result = * (size_t*) context->recvTaskResult(tid, sid, rSize);
			if ( (op & 0xff) & (OP_MapByKey | OP_FlatMap) )
				fddSize += result;
		}
		if ( (op & 0xff) & (OP_MapByKey | OP_FlatMap) )
			newFdd->setSize(fddSize);

		std::cerr << "  Done\n";
		return newFdd;
	}

	template <typename K>
	void groupedFdd<K>::cogroup(std::unordered_map<K, int> & keyMap){
		unsigned long int tid = context->enqueueTask(OP_CoGroup, id, this->size);
		void * result;
		size_t rSize;
		unsigned long int sid;

		context->sendKeyMap(tid, keyMap);

		for (size_t i = 1; i < context->numProcs(); ++i){
			result = context->recvTaskResult(tid, sid, rSize);
		}
	}

}

#endif
