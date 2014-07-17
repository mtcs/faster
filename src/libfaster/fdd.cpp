#include <omp.h>

#include "fdd.h"
#include "fastComm.h"
#include "fastCommBuffer.h"

template <typename T>
fddCore<T>::fddCore(fastContext & c){
	context = &c;
}

template <typename T>
fddCore<T>::fddCore(fastContext &c, size_t s) : fddCore(c){
	this->size = s;
}

template <typename T>
T fdd<T>::finishReduces(char ** partResult, size_t * pSize, int funcId, fddOpType op){
	fastCommBuffer buffer(0);
	T result;

	if (op == OP_Reduce){
		reduceFunctionP<T> reduceFunc = (reduceFunctionP<T>) this->context->funcTable[funcId];


		// Get the real object behind the buffer
		buffer.setBuffer(partResult[0], pSize[0]);
		buffer >> result;

		for (int i = 1; i < (this->context->numProcs() - 1); ++i){
			T pr;

			buffer.setBuffer(partResult[i], pSize[i]);
			buffer >> pr;

			result = reduceFunc(result, pr);
		}
	}else{
		bulkReduceFunctionP<T> bulkReduceFunc = (bulkReduceFunctionP<T>) this->context->funcTable[funcId];
		T * vals = new T[this->context->numProcs() - 1];

		#pragma omp parallel for
		for (int i = 1; i < (this->context->numProcs() - 1); ++i){
			fastCommBuffer buffer(0);

			buffer.setBuffer(partResult[i], pSize[i]);
			buffer >> vals[i];
		}

		result = bulkReduceFunc(vals, this->context->numProcs() - 1);
		delete [] vals;
		// TODO do bulkreduce	
	}
	return result;
}

template <typename T>
T fdd<T>::reduce( void * funcP, fddOpType op){
	//T fddCore<T>::template reduce( int funcId, fddOpType op){
	T result;
	unsigned long int tId;
	int funcId = this->context->findFunc(funcP);
	//std::cerr << " " << funcId << ".\n";
	char ** partResult = new char*[this->context->numProcs() - 1];
	size_t * partrSize = new size_t[this->context->numProcs() - 1];

	// Send task
	//unsigned long int reduceTaskId = 
	this->context->enqueueTask(op, this->id, 0, funcId);

	// Receive results
	for (int i = 0; i < (this->context->numProcs() - 1); ++i){
		char * pr = (char*) this->context->recvTaskResult(tId, partrSize[i]);
		partResult[i] = new char [ partrSize[i]];
		memcpy(partResult[i], pr, partrSize[i]);
	}

	// Finish applying reduces
	result = finishReduces(partResult, partrSize, funcId, op);

	for (int i = 0; i < (this->context->numProcs() - 1); ++i){
		delete [] partResult[i];
	}
	delete [] partResult;

	return result;
}

template <typename T>
std::vector<T> fdd<T*>::finishPReduces(T ** partResult, size_t * partrSize, int funcId, fddOpType op){
	T * result; 
	size_t rSize; 

	if (op == OP_Reduce){
		T * pResult;
		size_t prSize;

		PreducePFunctionP<T> reduceFunc = ( PreducePFunctionP<T> ) this->context->funcTable[funcId];

		result = partResult[0];
		rSize = partrSize[0];

		// Do the rest of the reduces
		for (int i = 1; i < (this->context->numProcs() - 1); ++i){
			// TODO
			pResult = result;
			prSize = rSize;
			std::pair<T*,size_t> r = reduceFunc(pResult, prSize, partResult[i], partrSize[i]);
			result = r.first;
			rSize = r.second; 
		}

	}else{
		PbulkReducePFunctionP<T> bulkReduceFunc = (PbulkReducePFunctionP<T>) this->context->funcTable[funcId];
		std::pair<T*,size_t> r = bulkReduceFunc(partResult, partrSize, this->context->numProcs() - 1);
		result = r.first;
		rSize = r.second; 
	}

	std::vector<T> vResult(rSize);
	vResult.assign(result,  result + rSize);

	return vResult;
}

template <typename T>
std::vector <T> fdd<T*>::reduceP(void * funcP, fddOpType op){
	//T fddCore<T>::template reduce( int funcId, fddOpType op){
	// Decode function pointer
	int funcId = this->context->findFunc(funcP);
	T ** partResult = new T*[this->context->numProcs() -1];
	size_t * partrSize = new size_t[this->context->numProcs() - 1];
	unsigned long int tId;
	//std::cerr << " " << funcId << ".\n";

	// Send task
	//unsigned long int reduceTaskId = 
	this->context->enqueueTask(op, this->id, 0, funcId);

	// Receive results
	for (int i = 0; i < (this->context->numProcs() - 1); ++i){
		partResult[i] = (T*) this->context->recvTaskResult(tId, partrSize[i]);
		partrSize[i] /= sizeof(T);
	}

	std::cerr << "\n ";

	// Finish applying reduces
	std::vector<T> vResult = finishPReduces(partResult, partrSize, funcId, op);

	delete [] partResult;
	delete [] partrSize;
	return vResult;
}


template class fddCore<char>;
template class fddCore<int>;
template class fddCore<long int>;
template class fddCore<float>;
template class fddCore<double>;
template class fddCore<char *>;
template class fddCore<int *>;
template class fddCore<long int *>;
template class fddCore<float *>;
template class fddCore<double *>;
template class fddCore<std::string>;
template class fddCore<std::vector<char>>;
template class fddCore<std::vector<int>>;
template class fddCore<std::vector<long int>>;
template class fddCore<std::vector<float>>;
template class fddCore<std::vector<double>>;




template <typename T>
fdd<T>::fdd(fastContext &c, const char * fileName) {
	fddCore<T>::context = &c;
	this->id = c.readFDD(this, fileName);

	// Recover FDD information (size, ? etc )
	this->context->getFDDInfo(this->size);
}



template class fdd<char>;
template class fdd<int>;
template class fdd<long int>;
template class fdd<float>;
template class fdd<double>;
template class fdd<char *>;
template class fdd<int *>;
template class fdd<long int *>;
template class fdd<float *>;
template class fdd<double *>;
template class fdd<std::string>;
template class fdd<std::vector<char>>;
template class fdd<std::vector<int>>;
template class fdd<std::vector<long int>>;
template class fdd<std::vector<float>>;
template class fdd<std::vector<double>>;
