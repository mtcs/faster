#ifndef LIBFASTER_FASTCOMMBUFFER_H
#define LIBFASTER_FASTCOMMBUFFER_H

#define BUFFER_INITIAL_SIZE 4*1024*1024

#include <vector>
#include <string>
#include <cstring>
#include <iostream>

class fastCommBuffer{
	private:
		char * _data;
		size_t _size;
		size_t _allocatedSize;
	public:
		fastCommBuffer();
		~fastCommBuffer();

		void reset();

		char * data();
		char * pos();
		size_t size();
		size_t free();
		void advance(size_t pos);

		void grow(size_t s);

		void print();

		// WRITE Data
		template <typename T>
		void write(T &v, size_t s){
			memcpy( &_data[_size], &v, s );
			_size += s;
		}

		template <typename T>
		void write(T * v, size_t s){
			memcpy( &_data[_size], v, s );
			_size += s;
		}

		template <typename T>
		void write(T v){
			write( v, sizeof(T) );
		}
		void write(std::string v){
			write( v.data(), v.size() );
		}
		template <typename T>
		void write(std::vector<T> v){
			write( v.data(), v.size() );
		}
		
		// READ Data
		template <typename T>
		void read(T & v, size_t s){
			memcpy(&v, &_data[_size], s );
			_size += s;
		}
		template <typename T>
		void read(T * v, size_t s){
			memcpy(v, &_data[_size], s );
			_size += s;
		}
		template <typename T>
		void read(T & v){
			read( v, sizeof(T) );
		}
		template <typename T>
		void read(std::vector<T> & v, size_t s){
			v.assign((T*) &_data[_size], s );
			_size += s;
		}
		void read(std::string & v, size_t s){
			v.assign( &_data[_size], s );
			_size += s;
		}

		// Operators

		template <typename T>
		fastCommBuffer & operator<<(T v){
			write(v);
			return *this;
		}

		template <typename T>
		fastCommBuffer & operator>>(T & v){
			read(v);
			return *this;
		}


};


#endif
