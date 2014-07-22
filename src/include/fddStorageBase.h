#ifndef LIBFASTER_FDDSTORAGEBASE_H
#define LIBFASTER_FDDSTORAGEBASE_H

#include <cstdlib>
#include <iostream>

#include "definitions.h"

namespace faster{
	class fddStorageBase{
		protected:
			size_t size;
			size_t allocSize;

		public:
			virtual ~fddStorageBase(){}

			virtual void grow(size_t toSize) = 0;

			size_t getSize(){ return size; }

			//void   setSize(size_t s){ this->grow(s); size = s;  }
			virtual void setSize(size_t s UNUSED) { std::cerr << "OPS! " << s; }
	};
}

#endif
