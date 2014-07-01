#include "fastCommBuffer.h"

fastCommBuffer::fastCommBuffer(){
	reset();
	_allocatedSize = BUFFER_INITIAL_SIZE;
	_data = new char [_allocatedSize];
}
fastCommBuffer::fastCommBuffer(size_t s){
	reset();
	if (s > 0){
		_allocatedSize = s;
		_data = new char [_allocatedSize];
	}else{
		_allocatedSize = 0;
		_data = NULL;
	}
}
fastCommBuffer::~fastCommBuffer(){
	delete [] _data;
}

void fastCommBuffer::setBuffer(void * buffer, size_t s){ 
	reset();
	_data = (char*) buffer;
	_allocatedSize = s;
}
void fastCommBuffer::reset(){ 
	_size = 0; 
}

char * fastCommBuffer::data(){ 
	return _data; 
}
char * fastCommBuffer::pos(){ 
	return &_data[_size]; 
}
size_t fastCommBuffer::size(){ 
	return _size; 
}
size_t fastCommBuffer::free(){ 
	return _allocatedSize - _size; 
}
void fastCommBuffer::advance(size_t pos){
	_size += pos; 
}

void fastCommBuffer::grow(size_t s){
	if (_allocatedSize < s){
		delete [] _data;
		_allocatedSize = std::max(size_t(1.5*_allocatedSize), s + _allocatedSize);
		_data = new char[_allocatedSize];
	}
}

void fastCommBuffer::print(){
	for (size_t i = 0; i < _size; ++i){
		std::cout << (int) _data[i] << ' ';
	}
}

