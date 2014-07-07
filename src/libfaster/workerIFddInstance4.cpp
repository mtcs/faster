#include "workerIFdd.cpp"
#include "workerIFddDependent.cpp"


template class workerIFdd<double, char>;
template class workerIFdd<double, int>;
template class workerIFdd<double, long int>;
template class workerIFdd<double, float>;
template class workerIFdd<double, double>;
template class workerIFdd<double, std::string>;
template class workerIFdd<double, std::vector<char>>;
template class workerIFdd<double, std::vector<int>>;
template class workerIFdd<double, std::vector<long int>>;
template class workerIFdd<double, std::vector<float>>;
template class workerIFdd<double, std::vector<double>>;

