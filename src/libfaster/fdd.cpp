#include "fdd.h"

template <typename T>
fddCore<T>::fddCore(fastContext & c){
	context = &c;
	fddBase::id = c.createFDD(this, typeid(T).hash_code() );
}

template <typename T>
fddCore<T>::fddCore(fastContext &c, size_t s){
	context = &c;
	fddBase::size = s;
	fddBase::id = c.createFDD(this,  typeid(T).hash_code(), s);
}

template <typename T>
T fddCore<T>::finishReduces(T * partResult, int funcId, fddOpType op){
	T result;
	if (op == OP_Reduce){
		reduceFunctionP<T> reduceFunc = (reduceFunctionP<T>) context->funcTable[funcId];
		result = partResult[0];
		for (int i = 1; i < (context->numProcs() - 1); ++i){
			result = reduceFunc(result, partResult[i]);
		}
	}else{
		bulkReduceFunctionP<T> bulkReduceFunc = (bulkReduceFunctionP<T>) context->funcTable[funcId];
		result = bulkReduceFunc(partResult, context->numProcs() - 1);
		// TODO do bulkreduce	
	}
	return result;
}

template <typename T>
T fddCore<T>::reduce( void * funcP, fddOpType op){
	//T fddCore<T>::template reduce( int funcId, fddOpType op){
	T result;
	size_t rSize;
	unsigned long int id;
	int funcId = context->findFunc(funcP);
	//std::cerr << " " << funcId << ".\n";
	T * partResult = new T[context->numProcs() - 1];

	// Send task
	unsigned long int reduceTaskId = context->enqueueTask(op, id, 0, funcId);

	// Receive results
	for (int i = 0; i < (context->numProcs() - 1); ++i){
		context->recvTaskResult(id, &partResult[i], rSize);
	}

	// Finish applying reduces
	finishReduces(partResult, funcId, op);


	delete [] partResult;
	return result;
}

template <typename T>
std::vector<T> fddCore<T>::finishPReduces(T ** partResult, size_t * partrSize, int funcId, fddOpType op){
	T * result; 
	size_t rSize; 

	if (op == OP_Reduce){
		T * pResult;
		size_t prSize;

		PreducePFunctionP<T> reduceFunc = ( PreducePFunctionP<T> ) context->funcTable[funcId];

		result = partResult[0];
		rSize = partrSize[0];

		// Do the rest of the reduces
		for (int i = 1; i < (context->numProcs() - 1); ++i){
			// TODO
			pResult = result;
			prSize = rSize;
			std::pair<T*,size_t> r = reduceFunc(pResult, prSize, partResult[i], partrSize[i]);
			result = r.first;
			rSize = r.second; 
			delete [] pResult;
		}

	}else{
		PbulkReducePFunctionP<T> bulkReduceFunc = (PbulkReducePFunctionP<T>) context->funcTable[funcId];
		std::pair<T*,size_t> r = bulkReduceFunc(partResult, partrSize, context->numProcs() - 1);
		result = r.first;
		rSize = r.second; 
	}

	std::vector<T> vResult(rSize);
	vResult.assign(result,  result + rSize);

	return vResult;
}

template <typename T>
std::vector <T> fddCore<T>::reduceP(void * funcP, fddOpType op){
	//T fddCore<T>::template reduce( int funcId, fddOpType op){
	// Decode function pointer
	int funcId = context->findFunc(funcP);
	T ** partResult = new T*[context->numProcs() -1];
	size_t * partrSize = new size_t[context->numProcs() - 1];
	unsigned long int id;
	//std::cerr << " " << funcId << ".\n";

	// Send task
	unsigned long int reduceTaskId = context->enqueueTask(op, id, 0, funcId);

	// Receive results
	for (int i = 0; i < (context->numProcs() - 1); ++i){
		context->recvTaskResult(id, &partResult[i], partrSize[i]);
	}

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
//template class fddCore<std::vector<char>>;
//template class fddCore<std::vector<int>>;
//template class fddCore<std::vector<long int>>;
//template class fddCore<std::vector<float>>;
//template class fddCore<std::vector<double>>;
//




template <typename T>
fdd<T>::fdd(fastContext &c, const char * fileName) {
	fddCore<T>::context = &c;
	fddBase::id = c.readFDD(this, fileName);

	// Recover FDD information (size, ? etc )
	fddCore<T>::context->getFDDInfo(fddBase::size);
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
//template class fdd<std::vector<char>>;
//template class fdd<std::vector<int>>;
//template class fdd<std::vector<long int>>;
//template class fdd<std::vector<float>>;
//template class fdd<std::vector<double>>;
