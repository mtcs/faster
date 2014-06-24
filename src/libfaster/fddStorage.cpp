#include "fddStorage.h"

template <> 
class fddStorage <std::string> {
	private:
		size_t size;
		size_t allocSize;

		std::string * localData;
	public:
		fddStorage(){
			allocSize = 200;
			localData = new std::string[allocSize];
			size = 0;
		}
		fddStorage(size_t s){
			allocSize = s;
			localData = new std::string[s];
			size = s;
		}
		fddStorage(std::string * data, size_t s) : fddStorage(s){
			memcpy(localData, data, s*sizeof(std::string) );
		}

		~fddStorage(){
			if (localData != NULL){
				delete [] localData;
			}
		}

		void setData( void * data, size_t s){
			grow(s / sizeof(std::string));
			//memcpy(localData, data, s );
			for ( size_t i = 0; i < s; ++i){
				localData[i] = ((std::string*) data)[s];
			}
			size = s / sizeof(std::string);
		}
		void setData( void ** data, size_t * lineSizes, size_t s){
			std::cerr << "ERROR: Something went wrong in the code\n";
		}

		std::string * getData(){ return localData; }
		size_t getSize(){ return size; }
		void   setSize(size_t s){ grow(s); size = s; }

		std::string & operator[](size_t ref){ return localData[ref]; }

		 void grow(size_t toSize){
			 if (allocSize < toSize){
			 	if ((allocSize * 1.8) > toSize){
					toSize = allocSize * 1.8;
				}
				
				std::string * newStorage = new std::string [toSize];

				if (size >0) 
					memcpy(newStorage, localData, size * sizeof( std::string ) );
				
				delete [] localData;
				
				localData = newStorage;
				allocSize = toSize;
			 }
		 }
		 void shrink(){
			 if ( (size > 0) && (allocSize > size) ){
				 std::string * newStorage = new std::string [size];
				
				memcpy(newStorage, localData, size * sizeof( std::string ) );

				delete [] localData;
				
				localData = newStorage;
				allocSize = size;
			 }
		 }

		 void insert(std::string & item){
			grow(size + 1);
			localData[size++] = item;	
		 }

};
