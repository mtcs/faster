#include "workerIFdd.cpp"
#include "workerIFddDependent.cpp"


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



