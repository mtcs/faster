#ifndef LIBFASTER_FASTCOMMBUFFER_H
#define LIBFASTER_FASTCOMMBUFFER_H

#define BUFFER_INITIAL_SIZE 4*1024*1024

#include <vector>
#include <string>
#include <cstring>
#include <iostream>
#include <tuple>

class fastCommBuffer{
	private:
		char * _data;
		size_t _size;
		size_t _allocatedSize;
	public:
		fastCommBuffer();
		fastCommBuffer(size_t s);
		~fastCommBuffer();

		void setBuffer(void * buffer, size_t s);
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
		void write(void * v, size_t s){
			memcpy( &_data[_size], v, s );
			_size += s;
		}

		template <typename T>
		void write(T v){
			write( v, sizeof(T) );
		}
		void write(std::string v){
			write( v.size() );
			write( v.data(), v.size() );
		}
		template <typename T>
		void write(std::vector<T> v){
			write( v.size() );
			write( v.data(), v.size() );
		}
		template <typename K, typename T>
		void write(std::pair<K,T> p){
			write(p.first);
			write(p.second);
		}
		template <typename K, typename T>
		void write(std::tuple<K,T, size_t> t){
			write(std::get<0>(t));
			write(std::get<1>(t));
			write(std::get<2>(t));
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
		void readVec(std::vector<T> & v, size_t s){
			v.assign((T*) &_data[_size], ((T*) &_data[_size]) + s );
			_size += s;
		}
		void readString(std::string & v, size_t s){
			v.assign( &_data[_size], &_data[_size] + s );
			_size += s;
		}
		template <typename T>
		void read(std::vector<T> & v){
			size_t size;
			read(size);
			readVec(v, size);
		}
		void read(std::string & s){
			size_t size;
			read(size);
			readString(s, size);
		}
		template <typename K, typename T>
		void read(std::pair<K,T> & p){
			K k;
			T t;
			read(k);
			read(t);
			p = std::make_pair (k,t);
		}
		template <typename K, typename T>
		void read(std::tuple<K,T, size_t> & t){
			K k;
			T type;
			size_t s;
			read(k);
			read(type);
			read(s);
			t = std::make_tuple (k,type,s);
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
