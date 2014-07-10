#include "workerIFdd.cpp"
#include "workerIFddDependent.cpp"


template class workerIFdd<std::string, char>;
template class workerIFdd<std::string, int>;
template class workerIFdd<std::string, long int>;
template class workerIFdd<std::string, float>;
template class workerIFdd<std::string, double>;
template class workerIFdd<std::string, std::string>;
