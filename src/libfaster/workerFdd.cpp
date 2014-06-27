#include "workerFdd.h"


// REDUCE
template <typename T>
T workerFdd<T>::reduce (reduceFunctionP<T> reduceFunc){
	T result;
	size_t s = localData.getSize();
	std::cerr << "START " << id << " " << s << " | ";

	#pragma omp parallel 
	{
		int nT = omp_get_num_threads();
		int tN = omp_get_thread_num();
		T partResult = localData[tN];

		#pragma omp master
		std::cerr << tN << "(" << nT << ")";

		#pragma omp for 
		for (int i = nT; i < s; ++i){
			partResult = reduceFunc(partResult, localData[i]);
		}
		#pragma omp master
		result = partResult;
		
		#pragma omp barrier
		
		#pragma omp critical
		if (omp_get_thread_num() != 0){
			result = reduceFunc(result, partResult);
		}
	}
	std::cerr << "END (RESULT:"<< result << ")";
	return result;
}

template <typename T>
T workerFdd<T>::bulkReduce (bulkReduceFunctionP<T> bulkReduceFunc){
	return bulkReduceFunc((T*) localData.getData(), localData.getSize());
}

template <typename T>
void workerFdd<T>::apply(void * func, fddOpType op, workerFddBase * dest, void * result, size_t & rSize){ 
	switch (dest->getType()){
		case Null: break;
		case Char:     _apply(func, op,  (workerFdd<char> *) dest, result, rSize); break;
		case Int:      _apply(func, op,  (workerFdd<int> *) dest, result, rSize); break;
		case LongInt:  _apply(func, op,  (workerFdd<long int> *) dest, result, rSize); break;
		case Float:    _apply(func, op,  (workerFdd<float> *) dest, result, rSize); break;
		case Double:   _apply(func, op,  (workerFdd<double> *) dest, result, rSize); break;
		case CharP:    _applyP(func, op, (workerFdd<char *> *) dest, result, rSize); break;
		case IntP:     _applyP(func, op, (workerFdd<int *> *) dest, result, rSize); break;
		case LongIntP: _applyP(func, op, (workerFdd<long int *> *) dest, result, rSize); break;
		case FloatP:   _applyP(func, op, (workerFdd<float *> *) dest, result, rSize); break;
		case DoubleP:  _applyP(func, op, (workerFdd<double *> *) dest, result, rSize); break;
		case String:   _apply(func, op,  (workerFdd<std::string> *) dest, result, rSize); break;
		//case Custom:   _apply(func, op, (workerFdd<void *> *) dest, result, rSize); break;
		//case CharV:     _apply(func, op, (workerFdd<std::vector<char>> *) dest, result, rSize); break;
		//case IntV:      _apply(func, op, (workerFdd<std::vector<int>> *) dest, result, rSize); break;
		//case LongIntV:  _apply(func, op, (workerFdd<std::vector<long int>> *) dest, result, rSize); break;
		//case FloatV:    _apply(func, op, (workerFdd<std::vector<float>> *) dest, result, rSize); break;
		//case DoubleV:   _apply(func, op, (workerFdd<std::vector<double>> *) dest, result, rSize); break;
	}
}

// REDUCE
template <typename T>
T * workerFdd<T *>::reduce (size_t & rSize, PreducePFunctionP<T> reduceFunc){
	T * result;
	size_t s = localData.getSize();
	//std::cerr << "START " << id << " " << s << " | ";

	#pragma omp parallel 
	{
		int nT = omp_get_num_threads();
		int tN = omp_get_thread_num();
		T * partResult = localData[tN];
		T * a, * b;
		size_t partRSize, aSize, bSize;

		#pragma omp master
		std::cerr << tN << "(" << nT << ")";

		#pragma omp for 
		for (int i = nT; i < s; ++i){
			a = partResult;
			b = localData[i];
			aSize = partRSize;
			bSize = localData.getLineSizes()[i];

			reduceFunc(partResult, partRSize, a, aSize, b, bSize);

			delete [] a;
			delete [] b;
		}
		#pragma omp master
		result = partResult;

		#pragma omp barrier

		#pragma omp critical
		if (omp_get_thread_num() != 0){
			a = result;
			b = partResult;
			aSize = rSize;
			bSize = partRSize;

			reduceFunc(result, rSize, a, aSize, b, bSize);

			delete [] a;
			delete [] b;
		}
	}
	//std::cerr << "END ";
	return result;
}

template <typename T>
T * workerFdd<T *>::bulkReduce (size_t & rSize, PbulkReducePFunctionP<T> bulkReduceFunc){
	T * result;
	bulkReduceFunc(result, rSize, (T**) localData.getData(), localData.getLineSizes(), localData.getSize());
	return result;
}


template <typename T>
void workerFdd<T *>::apply(void * func, fddOpType op, workerFddBase * dest, void * result, size_t & rSize){ 
	switch (dest->getType()){
		case Null: break;
		case Char:     _apply(func, op, (workerFdd<char> *) dest, result, rSize); break;
		case Int:      _apply(func, op, (workerFdd<int> *) dest, result, rSize); break;
		case LongInt:  _apply(func, op, (workerFdd<long int> *) dest, result, rSize); break;
		case Float:    _apply(func, op, (workerFdd<float> *) dest, result, rSize); break;
		case Double:   _apply(func, op, (workerFdd<double> *) dest, result, rSize); break;
		case CharP:    _applyP(func, op, (workerFdd<char *> *) dest, result, rSize); break;
		case IntP:     _applyP(func, op, (workerFdd<int *> *) dest, result, rSize); break;
		case LongIntP: _applyP(func, op, (workerFdd<long int *> *) dest, result, rSize); break;
		case FloatP:   _applyP(func, op, (workerFdd<float *> *) dest, result, rSize); break;
		case DoubleP:  _applyP(func, op, (workerFdd<double *> *) dest, result, rSize); break;
		case String:   _apply(func, op, (workerFdd<std::string> *) dest, result, rSize); break;
		//case Custom:   _apply(func, op, (workerFdd<void *> *) dest, result, rSize); break;
		//case CharV:     _apply(func, op, (workerFdd<std::vector<char>> *) dest, result, rSize); break;
		//case IntV:      _apply(func, op, (workerFdd<std::vector<int>> *) dest, result, rSize); break;
		//case LongIntV:  _apply(func, op, (workerFdd<std::vector<long int>> *) dest, result, rSize); break;
		//case FloatV:    _apply(func, op, (workerFdd<std::vector<float>> *) dest, result, rSize); break;
		//case DoubleV:   _apply(func, op, (workerFdd<std::vector<double>> *) dest, result, rSize); break;
	}
}

template class workerFdd<char>;
template class workerFdd<int>;
template class workerFdd<long int>;
template class workerFdd<float>;
template class workerFdd<double>;
template class workerFdd<char *>;
template class workerFdd<int *>;
template class workerFdd<long int *>;
template class workerFdd<float *>;
template class workerFdd<double *>;
template class workerFdd<std::string>;
//template class workerFdd<std::vector<char>>;
//template class workerFdd<std::vector<int>>;
//template class workerFdd<std::vector<long int>>;
//template class workerFdd<std::vector<float>>;
//template class workerFdd<std::vector<double>>;

