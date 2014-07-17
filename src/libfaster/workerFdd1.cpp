#include "workerFdd.cpp"

extern template class workerFdd<char>;
extern template class workerFdd<int>;
extern template class workerFdd<long int>;
extern template class workerFdd<float>;
extern template class workerFdd<double>;

extern template class workerFdd<std::string>;

extern template class workerFdd<char *>;
extern template class workerFdd<int *>;
extern template class workerFdd<long int *>;
extern template class workerFdd<float *>;
extern template class workerFdd<double *>;
//extern template class workerFdd<void *>;


template class workerFdd<std::vector<char>>;
template class workerFdd<std::vector<int>>;
template class workerFdd<std::vector<long int>>;
template class workerFdd<std::vector<float>>;
template class workerFdd<std::vector<double>>;

