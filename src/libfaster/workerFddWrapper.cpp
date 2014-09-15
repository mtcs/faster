#include <dlfcn.h>
#include <iostream>
#include <unordered_map>
#include <unordered_map>
#include "workerFdd.h"
#include "workerFddModule.h"

void * faster::workerFdd::dLHandler[3][7] = {};

// Key Type
std::unordered_map<faster::fddType, int> faster::workerFdd::khAssign = std::unordered_map<fddType, int> ({
		{0,0},{Char,1}, {Int,2}, {LongInt,3}, {Float,4}, {Double,5},{String,6}
		});
// Data Type
std::unordered_map<faster::fddType, int> faster::workerFdd::hAssign = std::unordered_map<fddType, int>({
		{Char,0}, {Int,0}, {LongInt,0}, {Float,0}, {Double,0},
		{CharP,1},{IntP,1},{LongIntP,1},{FloatP,1},{DoubleP,1},{Custom,1},
		{CharV,2},{IntV,2},{LongIntV,2},{FloatV,2},{DoubleV,2},{String,2},
	});

std::unordered_map<char, void *> faster::workerFdd::funcTable[3][7];


void * faster::workerFdd::load(const std::string libraryName){
	std::cerr << "[Loading " << libraryName ;
	
	void * hdlr = dlopen(libraryName.data(), RTLD_LAZY);
	//void * hdlr = dlopen(libraryName.data(), RTLD_NOW);
	
	if(hdlr == NULL){
		std::cerr << "\n\033[5m\033[91mERROR! \033[0m\033[38;5;202m"<< dlerror() << "\033[0m " << std::endl;
		exit(-1);
	}
	std::cerr << "]  ";

	return hdlr;
}

void faster::workerFdd::loadSym(dFuncName funcName, const std::string symbolName){
	void * symbl = dlsym(dLHandler[hAssign[type]][khAssign[keyType]], symbolName.data());

	if(symbl == NULL){
		std::cerr << "\n\033[5m\033[91mERROR! \033[0m\033[38;5;202m"<< dlerror() << "\033[0m " << std::endl;
		exit(-1);
	}
	funcTable[hAssign[type]] [khAssign[keyType]] [funcName] = symbl;
}

void faster::workerFdd::loadLib(){
	if ( dLHandler[hAssign[type]][0] )
		return;

	if (type & POINTER){
			dLHandler[hAssign[type]][0] = load("libfaster/libfasterWorkerPFdd.so");
	}else{
		if (type & (VECTOR | String))
			dLHandler[hAssign[type]][0] = load("libfaster/libfasterWorkerCFdd.so");
		else
			dLHandler[hAssign[type]][0] = load("libfaster/libfasterWorkerSFdd.so");
	}

}

void faster::workerFdd::loadLibI(){
	if ( dLHandler[hAssign[type]][khAssign[keyType]] )
		return;


	if (type & POINTER){
		switch (keyType){
			case Char:    dLHandler[hAssign[type]][khAssign[keyType]] = load("libfaster/libfasterWorkerIPFddInstance0.so"); break;
			case Int:     dLHandler[hAssign[type]][khAssign[keyType]] = load("libfaster/libfasterWorkerIPFddInstance1.so"); break;
			case LongInt: dLHandler[hAssign[type]][khAssign[keyType]] = load("libfaster/libfasterWorkerIPFddInstance2.so"); break;
			case Float:   dLHandler[hAssign[type]][khAssign[keyType]] = load("libfaster/libfasterWorkerIPFddInstance3.so"); break;
			case Double:  dLHandler[hAssign[type]][khAssign[keyType]] = load("libfaster/libfasterWorkerIPFddInstance4.so"); break;
			case String:  dLHandler[hAssign[type]][khAssign[keyType]] = load("libfaster/libfasterWorkerIPFddInstance5.so"); break;
		}
	}else{
		if (type & (VECTOR | String))
			switch (keyType){
				case Char:    dLHandler[hAssign[type]][khAssign[keyType]] = load("libfaster/libfasterWorkerIFddInstance6.so"); break;
				case Int:     dLHandler[hAssign[type]][khAssign[keyType]] = load("libfaster/libfasterWorkerIFddInstance7.so"); break;
				case LongInt: dLHandler[hAssign[type]][khAssign[keyType]] = load("libfaster/libfasterWorkerIFddInstance8.so"); break;
				case Float:   dLHandler[hAssign[type]][khAssign[keyType]] = load("libfaster/libfasterWorkerIFddInstance9.so"); break;
				case Double:  dLHandler[hAssign[type]][khAssign[keyType]] = load("libfaster/libfasterWorkerIFddInstance10.so"); break;
				case String:  dLHandler[hAssign[type]][khAssign[keyType]] = load("libfaster/libfasterWorkerIFddInstance11.so"); break;
			}
		else
			switch (keyType){
				case Char:    dLHandler[hAssign[type]][khAssign[keyType]] = load("libfaster/libfasterWorkerIFddInstance0.so"); break;
				case Int:     dLHandler[hAssign[type]][khAssign[keyType]] = load("libfaster/libfasterWorkerIFddInstance1.so"); break;
				case LongInt: dLHandler[hAssign[type]][khAssign[keyType]] = load("libfaster/libfasterWorkerIFddInstance2.so"); break;
				case Float:   dLHandler[hAssign[type]][khAssign[keyType]] = load("libfaster/libfasterWorkerIFddInstance3.so"); break;
				case Double:  dLHandler[hAssign[type]][khAssign[keyType]] = load("libfaster/libfasterWorkerIFddInstance4.so"); break;
				case String:  dLHandler[hAssign[type]][khAssign[keyType]] = load("libfaster/libfasterWorkerIFddInstance5.so"); break;
			}
	}

}


void faster::workerFdd::loadSymbols(){
	//funcTable[hAssign[t]] = std::unordered_map<char, void *>();
	if (funcTable[hAssign[type]] [khAssign[keyType]] .size() != 0)
		return;
	std::cerr << "[Locating Symbols (" << hAssign[type] << "," << khAssign[keyType] << ") ";
	std::cerr.flush();
	loadSym(NewWorkerSDL	, "newWorkerSDL");
	loadSym(DiscardWorkerDL	, "discardWorkerDL");
                                      
	loadSym(GetTypeDL	, "getTypeDL");
	loadSym(GetKeyTypeDL	, "getKeyTypeDL");
	                              
                                      
	loadSym(SetDataDL	, "setDataDL");
	loadSym(SetDataRawDL	, "setDataRawDL");
	std::cerr << ".";
                                      
	loadSym(GetLineSizesDL	, "getLineSizesDL");
                                      
	loadSym(GetFddItemDL	, "getFddItemDL");
	loadSym(GetKeysDL	, "getKeysDL");
	loadSym(GetDataDL	, "getDataDL");
	loadSym(GetSizeDL	, "getSizeDL");
	loadSym(ItemSizeDL	, "itemSizeDL");
	loadSym(BaseSizeDL	, "baseSizeDL");
	loadSym(SetSizeDL	, "setSizeDL");

	loadSym(DeleteItemDL	, "deleteItemDL");
	loadSym(ShrinkDL	, "shrinkDL");
	std::cerr << ".";
	                              
                                      
	loadSym(InsertDL	, "insertDL");
	loadSym(InsertListDL	, "insertListDL");
                                      
	loadSym(PreapplyDL	, "preapplyDL");
                                      
	loadSym(CollectDL	, "collectDL");
	loadSym(ExchangeDataByKeyDL, "exchangeDataByKeyDL");
	std::cerr << ".]  ";
}


// Create Indexed FDDs
faster::workerFdd::workerFdd(fddType t){
	type = t;
	keyType = 0;
	loadLib();
	loadSymbols();
}
faster::workerFdd::workerFdd(unsigned long int id, fddType t, size_t size) : workerFdd(t){
	this->id = id;
	void * funcP = funcTable[hAssign[t]] [0] [NewWorkerSDL];
	_fdd = ((workerFddBase * (*)(unsigned long int, fddType, size_t)) funcP)(id, t, size);
}
faster::workerFdd::workerFdd(unsigned long int id, fddType t) : workerFdd(id, t, size_t(0)){
}


// Create Indexed FDDs
faster::workerFdd::workerFdd(fddType kt, fddType t){
	type = t;
	keyType = kt;
	loadLibI();
	loadSymbols();
}
faster::workerFdd::workerFdd(unsigned long int id, fddType kt, fddType t, size_t size) : workerFdd(kt, t){
	this->id = id;
	void * funcP = funcTable[hAssign[t]] [khAssign[kt]] [NewWorkerSDL];
	_fdd = ((workerFddBase * (*)(unsigned long int, fddType, size_t)) funcP)(id, t, size);
}
faster::workerFdd::workerFdd(unsigned long int id, fddType kt, fddType t) : workerFdd(id, kt, t, 0){
}


faster::workerFdd::~workerFdd(){
	void * funcP =  funcTable[hAssign[type]] [khAssign[keyType]] [DiscardWorkerDL];
	((void (*) (workerFddBase *)) funcP)(_fdd);
}


faster::fddType faster::workerFdd::getType(){
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [GetTypeDL];
	return ((fddType (*)(workerFddBase *)) funcP)(_fdd);
}

faster::fddType faster::workerFdd::getKeyType(){
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [GetKeyTypeDL];
	return ((fddType (*)(workerFddBase *)) funcP)(_fdd);
}


//T & faster::workerFdd::operator[](size_t address){
void * faster::workerFdd::getItem(size_t address){
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [GetFddItemDL];
	return ((void * (*)(workerFddBase *, size_t)) funcP)(_fdd, address);
}

void * faster::workerFdd::getKeys(){
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [GetKeysDL];
	return ((void * (*)(workerFddBase * fdd)) funcP)(_fdd);
}

void * faster::workerFdd::getData(){
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [GetDataDL];
	return ((void * (*)(workerFddBase * fdd)) funcP)(_fdd);
}

size_t faster::workerFdd::getSize(){
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [GetSizeDL];
	return ((size_t (*)(workerFddBase *)) funcP)(_fdd);
}

size_t faster::workerFdd::itemSize(){
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [ItemSizeDL];
	return ((size_t (*)(workerFddBase *)) funcP)(_fdd);
}

size_t faster::workerFdd::baseSize(){
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [BaseSizeDL];
	return ((size_t (*)(workerFddBase *)) funcP)(_fdd);
}

void faster::workerFdd::setSize(size_t s){
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [SetSizeDL];
	return ((void (*)(workerFddBase *, size_t s)) funcP)(_fdd, s);
}


void faster::workerFdd::deleteItem(void * item){
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [DeleteItemDL];
	((void (*)(workerFddBase *, void*)) funcP)(_fdd, item);
}

void faster::workerFdd::shrink(){
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [ShrinkDL];
	((void (*)(workerFddBase *)) funcP)(_fdd);
}

// For known types
void faster::workerFdd::setData(void * data, size_t size) {
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [SetDataDL];
	((void (*)(workerFddBase *, void*, void*, size_t*, size_t)) funcP)(_fdd, NULL, data, NULL, size);
}

void faster::workerFdd::setData(void * data, size_t * lineSizes, size_t size) {
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [SetDataDL];
	((void (*)(workerFddBase *, void*, void*, size_t*, size_t)) funcP)(_fdd, NULL, data, lineSizes, size);
}
void faster::workerFdd::setData(void * keys, void * data, size_t size) {
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [SetDataDL];
	((void (*)(workerFddBase *, void*, void*, size_t*, size_t)) funcP)(_fdd, keys, data, NULL, size);
}

void faster::workerFdd::setData(void * keys, void * data, size_t * lineSizes, size_t size) {
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [SetDataDL];
	((void (*)(workerFddBase *, void*, void*, size_t*, size_t)) funcP)(_fdd, keys, data, lineSizes, size);
}

// For anonymous types

void faster::workerFdd::setDataRaw(void * data, size_t size){
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [SetDataRawDL];
	((void (*)(workerFddBase*, void*, void*, size_t*, size_t)) funcP)(_fdd, NULL, data, NULL, size);
}

void faster::workerFdd::setDataRaw(void * data, size_t *lineSizes, size_t size){
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [SetDataRawDL];
	((void (*)(workerFddBase*, void*, void*, size_t*, size_t)) funcP)(_fdd, NULL, data, lineSizes, size);
}
void faster::workerFdd::setDataRaw(void * keys, void * data, size_t size){
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [SetDataRawDL];
	((void (*)(workerFddBase*, void*, void*, size_t*, size_t)) funcP)(_fdd, keys, data, NULL, size);
}

void faster::workerFdd::setDataRaw(void * keys, void * data, size_t *lineSizes, size_t size){
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [SetDataRawDL];
	((void (*)(workerFddBase*, void*, void*, size_t*, size_t)) funcP)(_fdd, keys, data, lineSizes, size);
}


size_t * faster::workerFdd::getLineSizes(){
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [GetLineSizesDL];
	return ((size_t * (*)(workerFddBase *)) funcP)(_fdd);
}



void faster::workerFdd::insert(void * k, void * v, size_t s){
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [InsertDL];
	((void (*)(workerFddBase *, void *, void *, size_t)) funcP)(_fdd, k, v, s);
}

void faster::workerFdd::insertl(void * v){
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [InsertListDL];
	((void (*)(workerFddBase *, void *)) funcP)(_fdd, v);
}



// Apply task functions to FDDs
void faster::workerFdd::preapply(unsigned long int id, void * func, fddOpType op, workerFddBase * dest, fastComm * comm){
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [PreapplyDL];
	((void (*)(workerFddBase*, unsigned long int id, void*, fddOpType, workerFddBase*, fastComm *)) funcP)(_fdd, id, func, op, dest, comm);
}


void faster::workerFdd::collect(fastComm * comm){
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [CollectDL];
	((void (*)(workerFddBase *, fastComm *)) funcP)(_fdd, comm);
}
void faster::workerFdd::exchangeDataByKey(fastComm *comm, void * keyMap){
	void * funcP = funcTable[hAssign[type]] [khAssign[keyType]] [ExchangeDataByKeyDL];
	((void (*)(workerFddBase *, fastComm *, void*)) funcP)(_fdd, comm, keyMap);
}


