#include "workerFdd.cpp"

extern template class faster::workerFdd<char>;
extern template class faster::workerFdd<int>;
extern template class faster::workerFdd<long int>;
extern template class faster::workerFdd<float>;
extern template class faster::workerFdd<double>;

extern template class faster::workerFdd<std::string>;

extern template class faster::workerFdd<char *>;
extern template class faster::workerFdd<int *>;
extern template class faster::workerFdd<long int *>;
extern template class faster::workerFdd<float *>;
extern template class faster::workerFdd<double *>;
//extern template class faster::workerFdd<void *>;


template class faster::workerFdd<std::vector<char>>;
template class faster::workerFdd<std::vector<int>>;
template class faster::workerFdd<std::vector<long int>>;
template class faster::workerFdd<std::vector<float>>;
template class faster::workerFdd<std::vector<double>>;

