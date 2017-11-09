#ifndef LIBFASTER_FDDBASE_H
#define LIBFASTER_FDDBASE_H


#include "definitions.h"

namespace faster{
	template <typename K, typename T>
	class iFddCore;

	class fddBase{
		template <typename K, typename T>
		friend class iFddCore;
		protected:
			fddType _kType;
			fddType _tType;
			unsigned long int id;
			unsigned long int totalBlocks;
			unsigned long int size;
			std::vector<size_t> dataAlloc;
			bool cached;
		public:
			fddBase(): size(0){ }
			virtual ~fddBase() {}

			void setSize(size_t &s){ size = s; } // TODO Should be protected?

			/// @brief Returns the size of the dataset
			size_t getSize(){ return size; }

			/// @brief Returns the identification number of the dataset
			int getId(){return id;}

			/// @brief Returns the allocation identification number of the dataset
			const std::vector<size_t> & getAlloc(){ return dataAlloc; }

			fddType tType(){ return _tType; }
			fddType kType(){ return _kType; }

			/// @brief Returns true if the dataset is cached
			bool isCached(){ return cached; }

			virtual void discard() = 0;
			virtual bool isGroupedByKey() = 0;
			virtual void setGroupedByKey(bool gbk) = 0;

			//virtual void * _collect() = 0;
	};
}

#endif
