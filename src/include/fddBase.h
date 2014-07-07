#ifndef LIBFASTER_FDDBASE_H
#define LIBFASTER_FDDBASE_H


#include "definitions.h"


class fddBase{
	protected:
		fddType type;
		unsigned long int id;
		unsigned long int totalBlocks;
		unsigned long int size;
	public:
		void setSize(size_t &s){ size = s; }
		size_t getSize(){ return size; }
		int getId(){return id;}

		//virtual void * _collect() = 0;
};


#endif
