#include "workerIFdd.cpp"
#include "workerIFddDependent.cpp"


template class workerIFdd<char, std::vector<char>>;
template class workerIFdd<char, std::vector<int>>;
template class workerIFdd<char, std::vector<long int>>;
template class workerIFdd<char, std::vector<float>>;
template class workerIFdd<char, std::vector<double>>;

