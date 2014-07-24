#include <dlfcn.h>
#include <iostream>
#include <unordered_map>
#include <unordered_map>
#include "workerFdd.h"
#include "workerFddModule.h"

void * load(const std::string libraryName){
	std::cerr << "[Loading " << libraryName << "]  ";
	
	void * hdlr = dlopen(libraryName.data(), RTLD_LAZY);
	//void * hdlr = dlopen(libraryName.data(), RTLD_NOW);
	
	if(hdlr == NULL){
		std::cerr << "\n\033[5m\033[91mERROR!\033[0m\033[38;5;202m"<< dlerror() << "\033[0m " << std::endl;
		exit(-1);
	}

	return hdlr;
}

void * loadSymbol(void * hdlr, const std::string symbolName){
	void * symbl = dlsym(hdlr, symbolName.data());

	if(symbl == NULL){
		std::cerr << "\n\033[5m\033[91mERROR!\033[0m\033[38;5;202m"<< dlerror() << "\033[0m " << std::endl;
		exit(-1);
	}

	return symbl;
}

void * faster::workerFddWrapper::dLHandler[3] = {0,0,0};

std::unordered_map<faster::fddType, int> faster::workerFddWrapper::hAssign = std::unordered_map<fddType, int>({
		{Char,0}, {Int,0}, {LongInt,0}, {Float,0}, {Double,0},
		{CharP,1},{IntP,1},{LongIntP,1},{FloatP,1},{DoubleP,1},{Custom,1},
		{CharV,2},{IntV,2},{LongIntV,2},{FloatV,2},{DoubleV,2},{String,2},
	});

std::unordered_map<char, void *> faster::workerFddWrapper::funcTable[3];

//template <class T>
void faster::workerFdd::loadLib(const fddType t){

	std::cerr << " (" << dLHandler[hAssign[t]] ;
	if ( dLHandler[hAssign[t]] )
		return;

	switch (t){
		case Char:
		case Int:
		case LongInt:
		case Float:
		case Double:
			dLHandler[hAssign[t]] = load("libfaster/libfasterWorkerSFdd.so");
			break;
		case CharP:
		case IntP:
		case LongIntP:
		case FloatP:
		case DoubleP:
		case Custom:
			dLHandler[hAssign[t]] = load("libfaster/libfasterWorkerPFdd.so");
			break;
		case String:
		case CharV:
		case IntV:
		case LongIntV:
		case FloatV:
		case DoubleV:
			dLHandler[hAssign[t]] = load("libfaster/libfasterWorkerCFdd.so");
			break;
	}
	std::cerr  << dLHandler[hAssign[t]] << " )";

}

//template <class T>
void faster::workerFdd::loadSymbols(const fddType t){
	//funcTable[hAssign[t]] = std::unordered_map<char, void *>();
	if (funcTable[hAssign[t]].size() != 0)
		return;
	std::cerr << "[Locating Symbols";
	funcTable[hAssign[t]][NewWorkerDL]		= loadSymbol(dLHandler[hAssign[t]], "newWorkerDL");
	funcTable[hAssign[t]][NewWorkerSDL]		= loadSymbol(dLHandler[hAssign[t]], "newWorkerSDL");
	funcTable[hAssign[t]][DestroyWorkerDL]		= loadSymbol(dLHandler[hAssign[t]], "destroyWorkerDL");

	funcTable[hAssign[t]][GetTypeDL]		= loadSymbol(dLHandler[hAssign[t]], "getTypeDL");
	funcTable[hAssign[t]][GetKeyTypeDL]		= loadSymbol(dLHandler[hAssign[t]], "getKeyTypeDL");
	std::cerr << ".";

	funcTable[hAssign[t]][SetDataDL]		= loadSymbol(dLHandler[hAssign[t]], "setDataDL");
	funcTable[hAssign[t]][SetDataRawDL]		= loadSymbol(dLHandler[hAssign[t]], "setDataRawDL");

	funcTable[hAssign[t]][GetLineSizesDL]		= loadSymbol(dLHandler[hAssign[t]], "getLineSizesDL");

	funcTable[hAssign[t]][GetFddItemDL]		= loadSymbol(dLHandler[hAssign[t]], "getFddItemDL");
	funcTable[hAssign[t]][GetDataDL]		= loadSymbol(dLHandler[hAssign[t]], "getDataDL");
	funcTable[hAssign[t]][GetSizeDL]		= loadSymbol(dLHandler[hAssign[t]], "getSizeDL");
	funcTable[hAssign[t]][ItemSizeDL]		= loadSymbol(dLHandler[hAssign[t]], "itemSizeDL");
	funcTable[hAssign[t]][BaseSizeDL]		= loadSymbol(dLHandler[hAssign[t]], "baseSizeDL");
	funcTable[hAssign[t]][DeleteItemDL]		= loadSymbol(dLHandler[hAssign[t]], "deleteItemDL");
	funcTable[hAssign[t]][ShrinkDL]			= loadSymbol(dLHandler[hAssign[t]], "shrinkDL");
	std::cerr << ".";

	funcTable[hAssign[t]][InsertDL]			= loadSymbol(dLHandler[hAssign[t]], "insertDL");
	funcTable[hAssign[t]][InsertListDL]		= loadSymbol(dLHandler[hAssign[t]], "insertListDL");

	funcTable[hAssign[t]][ApplyDL]			= loadSymbol(dLHandler[hAssign[t]], "applyDL");

	funcTable[hAssign[t]][CollectDL]		= loadSymbol(dLHandler[hAssign[t]], "collectDL");
	funcTable[hAssign[t]][GroupByKeyDL]		= loadSymbol(dLHandler[hAssign[t]], "groupByKeyDL");
	funcTable[hAssign[t]][CountByKeyDL]		= loadSymbol(dLHandler[hAssign[t]], "countByKeyDL");
	std::cerr << ".]  ";
}

//template <class T>
faster::workerFdd::workerFdd(fddType t){
	type = t;
	loadLib(t);
	loadSymbols(t);
}

//template <class T>
faster::workerFdd::workerFdd(unsigned int ident, fddType t) : workerFdd(t){
	_fdd = ((workerFddBase * (*)(unsigned int, fddType)) funcTable[hAssign[t]][NewWorkerDL])(ident, t);
}
//template <class T>
faster::workerFdd::workerFdd(unsigned int ident, fddType t, size_t size) : workerFdd(t){
	//funcTable.reserve(50);
	void * func = funcTable[hAssign[t]][NewWorkerSDL];
	if (! func ) {
		std::cerr << "\n\033[5m\033[91mERROR!\033[0m\033[38;5;202m Could not find symbol NewWorkerSDL ("<< NewWorkerSDL  << ")\033[0m " << std::endl;
		exit(-1);
	}
	_fdd = ((workerFddBase * (*)(unsigned int, fddType, size_t)) funcTable[hAssign[t]][NewWorkerSDL])(ident, t, size);
}
//template <class T>
faster::workerFdd::~workerFdd(){
	((void (*) (workerFddBase *)) funcTable[hAssign[type]][DestroyWorkerDL])(_fdd);
}

//template <class T>
faster::fddType faster::workerFdd::getType(){
	return ((fddType (*)(workerFddBase *)) funcTable[hAssign[type]][GetTypeDL])(_fdd);
}
//template <class T>
faster::fddType faster::workerFdd::getKeyType(){
	return ((fddType (*)(workerFddBase *)) funcTable[hAssign[type]][GetKeyTypeDL])(_fdd);
}

//template <class T>
//T & faster::workerFdd::operator[](size_t address){
void * faster::workerFdd::getItem(size_t address){
	return ((void * (*)(workerFddBase *, size_t)) funcTable[hAssign[type]][GetFddItemDL])(_fdd, address);
}
//template <class T>
void * faster::workerFdd::getData(){
	return ((void * (*)(workerFddBase * fdd)) funcTable[hAssign[type]][GetDataDL])(_fdd);
}
//template <class T>
size_t faster::workerFdd::getSize(){
	return ((size_t (*)(workerFddBase *)) funcTable[hAssign[type]][GetSizeDL])(_fdd);
}
//template <class T>
size_t faster::workerFdd::itemSize(){
	return ((size_t (*)(workerFddBase *)) funcTable[hAssign[type]][ItemSizeDL])(_fdd);
}
//template <class T>
size_t faster::workerFdd::baseSize(){
	return ((size_t (*)(workerFddBase *)) funcTable[hAssign[type]][BaseSizeDL])(_fdd);
}

//template <class T>
void faster::workerFdd::deleteItem(void * item){
	((void (*)(workerFddBase *, void*)) funcTable[hAssign[type]][DeleteItemDL])(_fdd, item);
}
//template <class T>
void faster::workerFdd::shrink(){
	((void (*)(workerFddBase *)) funcTable[hAssign[type]][ShrinkDL])(_fdd);
}

// For known types
//template <class T>
void faster::workerFdd::setData(void * data, size_t size) {
	((void (*)(workerFddBase *, void*, void*, size_t*, size_t)) funcTable[hAssign[type]][SetDataDL])(_fdd, NULL, data, NULL, size);
}
//template <class T>
void faster::workerFdd::setData(void * data, size_t * lineSizes, size_t size) {
	((void (*)(workerFddBase *, void*, void*, size_t*, size_t)) funcTable[hAssign[type]][SetDataDL])(_fdd, NULL, data, lineSizes, size);
}

// For anonymous types
//template <class T>
void faster::workerFdd::setDataRaw(void * data, size_t size){
	((void (*)(workerFddBase*, void*, void*, size_t*, size_t)) funcTable[hAssign[type]][SetDataRawDL])(_fdd, NULL, data, NULL, size);
}
//template <class T>
void faster::workerFdd::setDataRaw(void * data, size_t *lineSizes, size_t size){
	((void (*)(workerFddBase*, void*, void*, size_t*, size_t)) funcTable[hAssign[type]][SetDataRawDL])(_fdd, NULL, data, lineSizes, size);
}

//template <class T>
size_t * faster::workerFdd::getLineSizes(){
	return ((size_t * (*)(workerFddBase *)) funcTable[hAssign[type]][GetLineSizesDL])(_fdd);
}


//template <class T>
void faster::workerFdd::insert(void * v, size_t s){
	((void (*)(workerFddBase *, void *, size_t)) funcTable[hAssign[type]][InsertDL])(_fdd, &v, s);
}
//template <class T>
void faster::workerFdd::insertl(void * v){
	((void (*)(workerFddBase *, void *)) funcTable[hAssign[type]][InsertListDL])(_fdd, &v);
}


/*
//template <class T>
void faster::workerFdd::insert(T & v){
	((void (*)(workerFddBase *, void *, size_t)) funcTable[hAssign[type]][InsertDL])(_fdd, &v, 0);
}
//template <class T>
void faster::workerFdd::insert(T & v, size_t s){
	((void (*)(workerFddBase *, void *, size_t)) funcTable[hAssign[type]][InsertDL])(_fdd, &v, s);
}
//template <class T>
void faster::workerFdd::insert(std::list<T> & in){
	((void (*)(workerFddBase *, void *)) funcTable[hAssign[type]][InsertListDL])(_fdd, &in);
}
//template <class T>
void faster::workerFdd::insert(std::list< std::pair<T, size_t> > & in){
	((void (*)(workerFddBase *, void *)) funcTable[hAssign[type]][InsertListDL])(_fdd, &in);
}// */


// Apply task functions to FDDs
//template <class T>
void faster::workerFdd::apply(void * func, fddOpType op, workerFddBase * dest, void *& result, size_t & rSize){
	((void (*)(workerFddBase*, void*, fddOpType, workerFddBase*, void**, size_t*)) funcTable[hAssign[type]][ApplyDL])(_fdd, func, op, dest, &result, &rSize);
}

//template <class T>
void faster::workerFdd::collect(fastComm * comm){
	((void (*)(workerFddBase *, fastComm *)) funcTable[hAssign[type]][CollectDL])(_fdd, comm);
}

//template <class T>
void faster::workerFdd::groupByKey(fastComm *comm){
	((void (*)(workerFddBase *, fastComm *)) funcTable[hAssign[type]][GroupByKeyDL])(_fdd, comm);
}
//template <class T>
void faster::workerFdd::countByKey(fastComm *comm){
	((void (*)(workerFddBase *, fastComm *)) funcTable[hAssign[type]][CountByKeyDL])(_fdd, comm);
}

/*
template class faster::workerFdd<char>;
template class faster::workerFdd<int>;
template class faster::workerFdd<long int>;
template class faster::workerFdd<float>;
template class faster::workerFdd<double>;

template class faster::workerFdd<char*>;
template class faster::workerFdd<int*>;
template class faster::workerFdd<long int*>;
template class faster::workerFdd<float*>;
template class faster::workerFdd<double*>;

template class faster::workerFdd<std::string>;

template class faster::workerFdd<std::vector<char>>;
template class faster::workerFdd<std::vector<int>>;
template class faster::workerFdd<std::vector<long int>>;
template class faster::workerFdd<std::vector<float>>;
template class faster::workerFdd<std::vector<double>>;
*/
