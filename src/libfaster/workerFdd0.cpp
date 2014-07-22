#include "workerFdd.cpp"

template class faster::workerFdd<char>;
template class faster::workerFdd<int>;
template class faster::workerFdd<long int>;
template class faster::workerFdd<float>;
template class faster::workerFdd<double>;

template class faster::workerFdd<std::string>;

extern template class faster::workerFdd<char *>;
extern template class faster::workerFdd<int *>;
extern template class faster::workerFdd<long int *>;
extern template class faster::workerFdd<float *>;
extern template class faster::workerFdd<double *>;
//extern template class faster::workerFdd<void *>;

extern template class faster::workerFdd<std::vector<char>>;
extern template class faster::workerFdd<std::vector<int>>;
extern template class faster::workerFdd<std::vector<long int>>;
extern template class faster::workerFdd<std::vector<float>>;
extern template class faster::workerFdd<std::vector<double>>;

