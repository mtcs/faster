#ifndef LIBFASTER_WORKERFDDMODULE_H
#define LIBFASTER_WORKERFDDMODULE_H

namespace faster{

	
	// C-stile Dynamic loaded functions
	extern "C"{
		workerFddBase * newWorkerSDL(unsigned long int ident, fddType type, size_t size);
		void destroyWorkerDL(workerFddBase * fdd);

		fddType getTypeDL(workerFddBase * fdd);
		fddType getKeyTypeDL(workerFddBase * fdd);

		void setDataDL(workerFddBase * fdd, void * keys, void * data, size_t * lineSizes, size_t size);
		void setDataRawDL(workerFddBase * fdd, void * keys, void * data, size_t * lineSizes, size_t size);

		size_t * getLineSizesDL(workerFddBase * fdd);

		void * getFddItemDL(workerFddBase * fdd, size_t address);
		void * getDataDL(workerFddBase * fdd);
		size_t getSizeDL(workerFddBase * fdd);
		size_t itemSizeDL(workerFddBase * fdd);
		size_t baseSizeDL(workerFddBase * fdd);
		void deleteItemDL(workerFddBase * fdd, void * item);
		void shrinkDL(workerFddBase * fdd);

		void insertDL(workerFddBase * fdd, void * k, void * v, size_t s);
		void insertListDL(workerFddBase * fdd, void * v);

		void applyDL(workerFddBase * fdd, void * func, fddOpType op, workerFddBase * dest, void ** result, size_t * rSize);

		void collectDL(workerFddBase * fdd, fastComm * comm);
		void groupByKeyDL(workerFddBase * fdd, fastComm * comm);
		void countByKeyDL(workerFddBase * fdd, fastComm * comm);
	}
}

#endif
