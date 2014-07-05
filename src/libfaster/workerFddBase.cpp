#include "workerFddBase.h"
#include "fastCommBuffer.h"

workerFddBase::workerFddBase() {
	resultBuffer = new fastCommBuffer();
}
workerFddBase::workerFddBase(unsigned int ident, fddType t) : id(ident), type(t) {
	resultBuffer = new fastCommBuffer();
}

workerFddBase::~workerFddBase() {
	delete resultBuffer;
};


