#ifndef LIBFASTER_WORKERFDDBASE_H
#define LIBFASTER_WORKERFDDBASE_H

class fastCommBuffer;
class fastComm;

#include <cstdlib>

#include "definitions.h"

class workerFddBase{
	protected:
		unsigned long int id;
		fddType type;
		fddType keyType;
		fastCommBuffer * resultBuffer;

	public:
		workerFddBase() ;
		workerFddBase(unsigned int ident, fddType t);
		virtual ~workerFddBase() ;

		virtual fddType getType() = 0;
		virtual fddType getKeyType() = 0;

		virtual void setDataRaw( void *, size_t) = 0;
		virtual void setDataRaw( void *, size_t *, size_t) = 0;
		virtual void setDataRaw( void *, void *, size_t) = 0;
		virtual void setDataRaw( void *, void *, size_t *, size_t) = 0;

		virtual void * getData() = 0;
		virtual size_t getSize() = 0;

		virtual size_t itemSize() = 0;
		virtual size_t baseSize() = 0;

		virtual void deleteItem(void * item) = 0;

		virtual void apply(void * func, fddOpType op, workerFddBase * dest, void *& result, size_t & rSize) = 0;

		virtual void collect(fastComm * comm) = 0;
		virtual void groupByKey(fastComm * comm) = 0;
		virtual void countByKey(fastComm * comm) = 0;
				
};

#endif
