//#include "fddStorageExtern.cpp"
#include "workerFddBase.h"
#include "workerFddModule.h"

void faster::destroyWorkerDL(faster::workerFddBase * fdd){
	delete fdd;
}

faster::fddType faster::getTypeDL(faster::workerFddBase * fdd){
	return fdd->getType();
}
faster::fddType faster::getKeyTypeDL(faster::workerFddBase * fdd){
	return fdd->getKeyType();
}

void faster::setDataDL(workerFddBase * fdd, void * keys, void * data, size_t * lineSizes, size_t size){
	if (keys){
		if (lineSizes)
			fdd->setData(keys, data, lineSizes, size);
		else
			fdd->setData(keys, data, size);
	}else{
		if (lineSizes)
			fdd->setData(data, lineSizes, size);
		else
			fdd->setData(data, size);
	}
}
void faster::setDataRawDL(workerFddBase * fdd, void * keys, void * data, size_t * lineSizes, size_t size){
	if (keys){
		if (lineSizes)
			fdd->setDataRaw(keys, data, lineSizes, size);
		else
			fdd->setDataRaw(keys, data, size);
	}else{
		if (lineSizes)
			fdd->setDataRaw(data, lineSizes, size);
		else
			fdd->setDataRaw(data, size);
	}
}

size_t * faster::getLineSizesDL(workerFddBase * fdd){
	return fdd->getLineSizes();
}

void * faster::getFddItemDL(workerFddBase * fdd, size_t address){
	return fdd->getItem(address);
}
void * faster::getDataDL(workerFddBase * fdd){
	return fdd->getData();
}
size_t faster::getSizeDL(workerFddBase * fdd){
	return fdd->getSize();
}
size_t faster::itemSizeDL(workerFddBase * fdd){
	return fdd->itemSize();
}
size_t faster::baseSizeDL(workerFddBase * fdd){
	return fdd->baseSize();
}
void faster::deleteItemDL(workerFddBase * fdd, void * item){
	fdd->deleteItem(item);
}
void faster::shrinkDL(workerFddBase * fdd){
	fdd->shrink();
}

void faster::insertDL(workerFddBase * fdd, void * k, void * v, size_t s){
	fdd->insert(k, v, s);
}
void faster::insertListDL(workerFddBase * fdd, void * v){
	fdd->insertl(v);
}

void faster::applyDL(workerFddBase * fdd, void * func, fddOpType op, workerFddBase * dest, void ** result, size_t * rSize){
	fdd->apply(func, op, dest, *result, *rSize);
}

void faster::collectDL(workerFddBase * fdd, fastComm * comm){
	fdd->collect(comm);
}
void faster::groupByKeyDL(workerFddBase * fdd, fastComm * comm){
	fdd->groupByKey(comm);
}
void faster::countByKeyDL(workerFddBase * fdd, fastComm * comm){
	fdd->countByKey(comm);
}


