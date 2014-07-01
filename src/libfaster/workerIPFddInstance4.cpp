#include "workerIPFdd.cpp"
#include "workerIPFddDependent.cpp"


template class workerIFdd<double, char *>;
template class workerIFdd<double, int *>;
template class workerIFdd<double, long int *>;
template class workerIFdd<double, float *>;
template class workerIFdd<double, double *>;

