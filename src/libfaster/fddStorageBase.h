#ifndef LIBFASTER_FDDSTORAGEBASE_H
#define LIBFASTER_FDDSTORAGEBASE_H

#include <cstdlib>

class fddStorageBase{
	protected:
		size_t size;
		size_t allocSize;

	public:
		virtual void grow(size_t toSize){ }
		size_t getSize(){ return size; }
		void   setSize(size_t s){ grow(s); size = s; }
};


#endif
