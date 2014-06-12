#ifndef LIBFASTER_FDDBASE_H
#define LIBFASTER_FDDBASE_H


enum fddType{
	Null,
	Int,
	LongInt,
	Float,
	Double,
	String,
	Object
};

enum fddOpType{
	Map,
	Reduce,
};


class fddBase{
	protected:
		fddType type;
		unsigned long int id;
		unsigned long int totalBlocks;
		unsigned long int size;
};


class fastTask{
	public:
		unsigned long int id;
		unsigned long int srcFDD;
		unsigned long int destFDD;
		fddOpType	operationType;
		unsigned int	functionId;
};

#endif
