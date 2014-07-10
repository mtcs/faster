#include "workerIFdd.cpp"
#include "workerIFddDependent.cpp"


template class workerIFdd<char, char>;
template class workerIFdd<char, int>;
template class workerIFdd<char, long int>;
template class workerIFdd<char, float>;
template class workerIFdd<char, double>;
template class workerIFdd<char, std::string>;


