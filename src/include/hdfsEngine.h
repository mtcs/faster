#ifndef LIBFASTER_HFDSENGINE_H
#define LIBFASTER_HFDSENGINE_H

#include <string>
#include <fcntl.h>

namespace faster{

	enum fileMode : int {
		R   = O_RDONLY,
		W   = O_WRONLY,
		RW  = O_RDWR,
		CR  = O_RDONLY | O_CREAT,
		CW  = O_WRONLY | O_CREAT,
		CRW = O_RDWR   | O_CREAT
	};


	class hdfsFile{
		private:
			void * _fs;
			void * _f;
			bool _open;

		public:
			hdfsFile(void * fs, std::string path, fileMode mode);
			~hdfsFile();

			void close();
	};

	class hdfsEngine{
		private:
			void * _fs;
			bool _ready;

		public:
			hdfsEngine();

			bool isReady();

			faster::hdfsFile open(std::string path, fileMode mode);
			void close(faster::hdfsFile & f);
	};
}

#endif
