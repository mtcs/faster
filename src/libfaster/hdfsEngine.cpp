#include <string>

#include "hdfs.h"
#include "hdfsEngine.h"

faster::hdfsEngine::hdfsEngine(){
	_ready = false;
	_fs = hdfsConnect("default", 0);
	_ready = true;
}

bool faster::hdfsEngine::isReady(){
	return _ready;
}

faster::hdfsFile faster::hdfsEngine::open(std::string path, fileMode mode){
	return hdfsFile((hdfsFS) _fs, path, mode);
}
void faster::hdfsEngine::close(faster::hdfsFile & f){
	f.close();
}

faster::hdfsFile::hdfsFile(void * fs, std::string path, fileMode mode){
	_fs = fs;
	_f = hdfsOpenFile((::hdfsFS) fs, path.data(), mode,0,0,0);
	_open = true;
}
faster::hdfsFile::~hdfsFile(){
	close();
}
void faster::hdfsFile::close(){
	if (_open){
		hdfsCloseFile((::hdfsFS) _fs, (::hdfsFile) _f);
		_open = false;
	}
}
