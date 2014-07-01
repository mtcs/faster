#include "workerIPFddFlatMap.cpp"

template class workerIFdd<double, char *>;
template class workerIFdd<double, int *>;
template class workerIFdd<double, long int *>;
template class workerIFdd<double, float *>;
template class workerIFdd<double, double *>;

template class workerIFdd<std::string, char *>;
template class workerIFdd<std::string, int *>;
template class workerIFdd<std::string, long int *>;
template class workerIFdd<std::string, float *>;
template class workerIFdd<std::string, double *>;
