#include "workerIFdd.cpp"
#include "workerIFddDependent.cpp"


template class workerIFdd<float, char>;
template class workerIFdd<float, int>;
template class workerIFdd<float, long int>;
template class workerIFdd<float, float>;
template class workerIFdd<float, double>;
template class workerIFdd<float, std::string>;

