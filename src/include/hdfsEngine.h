#ifndef LIBFASTER_HFDSENGINE_H
#define LIBFASTER_HFDSENGINE_H

#include <algorithm>
#include <string>
#include <vector>
#include <deque>
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

			// getLine
			const size_t _readBufferSize = 16*1024;
			char * _readBuffer;
			long int _readSize;
			size_t _readOffset;
			size_t _readPos;
			bool _eof;

		public:
			hdfsFile(void * fs, std::string & path, fileMode mode);
			~hdfsFile();

			void close();
			bool good();

			size_t read(char * v, size_t n);
			size_t write(char * v, size_t n);
			size_t seek(size_t offset);

			std::string getLine(size_t offset, char sep);
			std::string getLine(char sep);

			std::vector<std::deque<int>> getBlocksLocations();

			void del();
	};

	class hdfsEngine{
		private:
			void * _fs;
			bool _ready;
			std::string _path;

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
