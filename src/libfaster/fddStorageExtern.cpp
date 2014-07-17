#include "fddStorage.h"

extern template class fddStorage<char>;
extern template class fddStorage<int>;
extern template class fddStorage<long int>;
extern template class fddStorage<float>;
extern template class fddStorage<double>;

extern template class fddStorage<char *>;
extern template class fddStorage<int *>;
extern template class fddStorage<long int *>;
extern template class fddStorage<float *>;
extern template class fddStorage<double *>;

extern template class fddStorage<std::string>;

extern template class fddStorage<std::vector<char>>;
extern template class fddStorage<std::vector<int>>;
extern template class fddStorage<std::vector<long int>>;
extern template class fddStorage<std::vector<float>>;
extern template class fddStorage<std::vector<double>>;
