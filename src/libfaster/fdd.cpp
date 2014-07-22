#include <omp.h>

#include "fdd.h"
#include "fastComm.h"
#include "fastCommBuffer.h"

template class fddCore<char>;
template class fddCore<int>;
template class fddCore<long int>;
template class fddCore<float>;
template class fddCore<double>;
template class fddCore<char *>;
template class fddCore<int *>;
template class fddCore<long int *>;
template class fddCore<float *>;
template class fddCore<double *>;
template class fddCore<std::string>;
template class fddCore<std::vector<char>>;
template class fddCore<std::vector<int>>;
template class fddCore<std::vector<long int>>;
template class fddCore<std::vector<float>>;
template class fddCore<std::vector<double>>;






template class fdd<char>;
template class fdd<int>;
template class fdd<long int>;
template class fdd<float>;
template class fdd<double>;
template class fdd<char *>;
template class fdd<int *>;
template class fdd<long int *>;
template class fdd<float *>;
template class fdd<double *>;
template class fdd<std::string>;
template class fdd<std::vector<char>>;
template class fdd<std::vector<int>>;
template class fdd<std::vector<long int>>;
template class fdd<std::vector<float>>;
template class fdd<std::vector<double>>;
