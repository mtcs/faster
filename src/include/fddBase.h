#ifndef LIBFASTER_FDDBASE_H
#define LIBFASTER_FDDBASE_H


#include "definitions.h"

namespace faster{
	class fddBase{
		protected:
			fddType _kType;
			fddType _tType;
			unsigned long int id;
			unsigned long int totalBlocks;
			unsigned long int size;
			std::vector<size_t> dataAlloc;
			bool cached;
		public:
			void setSize(size_t &s){ size = s; }
			size_t getSize(){ return size; }
			int getId(){return id;}
			const std::vector<size_t> & getAlloc(){ return dataAlloc; }

			fddType tType(){ return _tType; }
			fddType kType(){ return _kType; }

			bool isCached(){ return cached; }

			virtual void discard() = 0;
			virtual void * getKeyMap(void) = 0;
			virtual void setKeyMap(void * keyMap) = 0;
			virtual bool isGroupedByKey() = 0;
			virtual void setGroupedByKey(bool gbk) = 0;

			//virtual void * _collect() = 0;
	};
}

#endif
