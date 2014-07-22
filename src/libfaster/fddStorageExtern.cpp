#include "fddStorage.h"

extern template class faster::fddStorage<char>;
extern template class faster::fddStorage<int>;
extern template class faster::fddStorage<long int>;
extern template class faster::fddStorage<float>;
extern template class faster::fddStorage<double>;

extern template class faster::fddStorage<char *>;
extern template class faster::fddStorage<int *>;
extern template class faster::fddStorage<long int *>;
extern template class faster::fddStorage<float *>;
extern template class faster::fddStorage<double *>;

extern template class faster::fddStorage<std::string>;

extern template class faster::fddStorage<std::vector<char>>;
extern template class faster::fddStorage<std::vector<int>>;
extern template class faster::fddStorage<std::vector<long int>>;
extern template class faster::fddStorage<std::vector<float>>;
extern template class faster::fddStorage<std::vector<double>>;
