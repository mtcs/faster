#ifndef LIBFASTER_FDDBASE_H
#define LIBFASTER_FDDBASE_H


enum fddType{
	Null,
	Char,
	Int,
	LongInt,
	Float,
	Double,
	String,
	Custom
};

enum fddOpType{
	Map,
	BulkMap,
	FlatMap,
	BulkFlatMap,
	Reduce,
	BulkReduce
};


class fddBase{
	protected:
		fddType type;
		unsigned long int id;
		unsigned long int totalBlocks;
		unsigned long int size;
	public:
		void setSize(size_t &s){ size = s; }
};


class fastTask{
	public:
		unsigned long int id;
		unsigned long int srcFDD;
		unsigned long int destFDD;
		fddOpType	operationType;
		unsigned int	functionId;
		void * result;
		size_t resultSize;
		size_t workersFinished;
};

#endif
