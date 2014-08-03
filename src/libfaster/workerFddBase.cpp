#include "workerFddBase.h"
#include "fastCommBuffer.h"


faster::workerFddBase::workerFddBase() {
}
faster::workerFddBase::workerFddBase(unsigned int ident, fddType t) : id(ident), type(t) {
}

faster::workerFddBase::~workerFddBase() {
};


