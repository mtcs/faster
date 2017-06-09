#ifndef LIBFASTER_HFDSENGINE_H
#define LIBFASTER_HFDSENGINE_H

#include <algorithm>
#include <string>
#include <vector>
#include <fcntl.h>

namespace faster{

	enum fileMode : int {
		R   = O_RDONLY,
		W   = O_WRONLY,
		//RW  = O_RDWR,
		CR  = O_RDONLY | O_CREAT,
		CW  = O_WRONLY | O_CREAT
		//CRW = O_RDWR   | O_CREAT
	};


	class hdfsFile{
		private:
			void * _fs;
			void * _f;
			bool _open;
			std::string _path;
			std::vector<char> _buffer;
			size_t read(char* v);

		public:
			hdfsFile(void * fs, std::string path, fileMode mode);
			~hdfsFile();

			void close();

			size_t read(char * v, size_t n);
			size_t write(char * v, size_t n);

			char get();

			void del();

			bool good();

			void seekg(size_t size, size_t offset);
	};

	class hdfsEngine{
		private:
			void * _fs;
			bool _ready;

		public:
			hdfsEngine();
			~hdfsEngine();

			bool isReady();
			bool isConnected();

			faster::hdfsFile open(std::string path, fileMode mode);
			void close(faster::hdfsFile & f);

			void del(std::string path);
			bool exists(std::string path);
	};
}

#endif
