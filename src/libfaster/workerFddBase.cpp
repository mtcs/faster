#include "workerFddBase.h"
#include "fastCommBuffer.h"


faster::workerFddBase::workerFddBase() {
	resultBuffer = new fastCommBuffer();
}
faster::workerFddBase::workerFddBase(unsigned int ident, fddType t) : id(ident), type(t) {
	resultBuffer = new fastCommBuffer();
}

faster::workerFddBase::~workerFddBase() {
	delete resultBuffer;
};


