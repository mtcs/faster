#ifndef LIBFASTER_WORKERFDDBASE_H
#define LIBFASTER_WORKERFDDBASE_H

#include <cstdlib>

#include "fddBase.h"

class workerFddBase{
	protected:
		unsigned long int id;
		fddType type;
		fddType keyType;

	public:
		workerFddBase(unsigned int ident, fddType t) : id(ident), type(t) {}
		
		virtual ~workerFddBase() {};

		virtual fddType getType() = 0;
		virtual fddType getKeyType() = 0;

		virtual void setData( void *, size_t) = 0;
		virtual void setData( void **, size_t *, size_t) = 0;
		virtual void setData( void *, void *, size_t) = 0;
		virtual void setData( void *, void **, size_t *, size_t) = 0;
		
		virtual void * getData() = 0;
		virtual size_t getSize() = 0;

		virtual size_t itemSize() = 0;
		virtual size_t baseSize() = 0;

		virtual void apply(void * func, fddOpType op, workerFddBase * dest, void * result, size_t & rSize) = 0;
				
};

#endif
