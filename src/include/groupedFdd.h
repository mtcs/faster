#ifndef LIBFASTER_GROUPEDFDD_H
#define LIBFASTER_GROUPEDFDD_H

template <class K, class T> 
class indexedFdd ; 

//#include "indexedFdd.h"
#include "fastContext.h"


template<typename... Types> 
class groupedFdd : fddBase{
	private:
		static const unsigned short int dimension = sizeof...(Types);
		fastContext * context;
		std::vector<fddBase *> members;

		template <typename K, typename T, typename... iFdds>
		void addFdds(indexedFdd<K,T> * fdd, iFdds ... otherFdds){
			members.insert(members.end(), fdd);

			addFdds(otherFdds...);
		}

	public:
		groupedFdd(fastContext * c, Types... args){
			context = c;

			addFdds(args...);
			//context->coPartition(members);
			id = context->createFddGroup(this, members); 
		}

};

#endif
