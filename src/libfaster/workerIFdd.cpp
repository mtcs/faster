#include <iostream>
#include "workerIFdd.h"





template <typename K, typename T>
void workerIFdd<K,T>::apply(void * func, fddOpType op, workerFddBase * dest, void * result, size_t & rSize){ 
	switch (op){
		case OP_Map:
		case OP_BulkMap:
			applyMap(func, op, dest);
		case OP_FlatMap:
		case OP_BulkFlatMap:
			applyFlatMap(func, op, dest);
		case OP_Reduce:
		case OP_BulkReduce:
			applyReduce(func, op, result, rSize);
	}
}


// -------------------------- Public Functions ------------------------ //

template <typename K, typename T>
void insert(K key, T & in){ 
	workerIFdd<K,T>::localData.insert(key, in); 
}

template <typename K, typename T>
void workerIFdd<K,T>::insert(std::list< std::pair<K, T> > & in){ 
	typename std::list< std::pair<K, T> >::iterator it;

	if (localData.getSize() < in.size())
		localData.grow(in.size());

	for ( it = in.begin(); it != in.end(); it++)
		localData.insert(it->first, it->second); 
}




template class workerIFdd<char, char>;
template class workerIFdd<char, int>;
template class workerIFdd<char, long int>;
template class workerIFdd<char, float>;
template class workerIFdd<char, double>;
template class workerIFdd<char, std::string>;
//template class workerIFdd<char, std::vector<char>>;
//template class workerIFdd<char, std::vector<int>>;
//template class workerIFdd<char, std::vector<long int>>;
//template class workerIFdd<char, std::vector<float>>;
//template class workerIFdd<char, std::vector<double>>;

template class workerIFdd<int, char>;
template class workerIFdd<int, int>;
template class workerIFdd<int, long int>;
template class workerIFdd<int, float>;
template class workerIFdd<int, double>;
template class workerIFdd<int, std::string>;
//template class workerIFdd<int, std::vector<char>>;
//template class workerIFdd<int, std::vector<int>>;
//template class workerIFdd<int, std::vector<long int>>;
//template class workerIFdd<int, std::vector<float>>;
//template class workerIFdd<int, std::vector<double>>;

template class workerIFdd<long int, char>;
template class workerIFdd<long int, int>;
template class workerIFdd<long int, long int>;
template class workerIFdd<long int, float>;
template class workerIFdd<long int, double>;
template class workerIFdd<long int, std::string>;
//template class workerIFdd<long int, std::vector<char>>;
//template class workerIFdd<long int, std::vector<int>>;
//template class workerIFdd<long int, std::vector<long int>>;
//template class workerIFdd<long int, std::vector<float>>;
//template class workerIFdd<long int, std::vector<double>>;

template class workerIFdd<float, char>;
template class workerIFdd<float, int>;
template class workerIFdd<float, long int>;
template class workerIFdd<float, float>;
template class workerIFdd<float, double>;
template class workerIFdd<float, std::string>;
//template class workerIFdd<float, std::vector<char>>;
//template class workerIFdd<float, std::vector<int>>;
//template class workerIFdd<float, std::vector<long int>>;
//template class workerIFdd<float, std::vector<float>>;
//template class workerIFdd<float, std::vector<double>>;

template class workerIFdd<double, char>;
template class workerIFdd<double, int>;
template class workerIFdd<double, long int>;
template class workerIFdd<double, float>;
template class workerIFdd<double, double>;
template class workerIFdd<double, std::string>;
//template class workerIFdd<double, std::vector<char>>;
//template class workerIFdd<double, std::vector<int>>;
//template class workerIFdd<double, std::vector<long int>>;
//template class workerIFdd<double, std::vector<float>>;
//template class workerIFdd<double, std::vector<double>>;

template class workerIFdd<std::string, char>;
template class workerIFdd<std::string, int>;
template class workerIFdd<std::string, long int>;
template class workerIFdd<std::string, float>;
template class workerIFdd<std::string, double>;
template class workerIFdd<std::string, std::string>;
//template class workerIFdd<std::string, std::vector<char>>;
//template class workerIFdd<std::string, std::vector<int>>;
//template class workerIFdd<std::string, std::vector<long int>>;
//template class workerIFdd<std::string, std::vector<float>>;
//template class workerIFdd<std::string, std::vector<double>>;


