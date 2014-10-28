#ifndef LIBFASTER_FASTCOMMBUFFER_H
#define LIBFASTER_FASTCOMMBUFFER_H


#include <vector>
#include <cstring>
#include <iostream>
#include <tuple>

namespace faster {
	const int BUFFER_INITIAL_SIZE = 512*1024;

	class fastCommBuffer{
		private:
			char * _data;
			size_t _size;
			size_t _allocatedSize;
			bool _ownData;

		public:
			fastCommBuffer();
			fastCommBuffer(size_t s);
			~fastCommBuffer();

			void setBuffer(void * buffer, size_t s);
			void reset();

			char * data();
			char * pos();
			char * pos(size_t pos);
			size_t size();
			size_t free();
			void advance(size_t pos);

			void grow(size_t s);

			void print();

			// WRITE Data
			template <typename T>
			void write(T &v, size_t s){
				grow(_size + s);
				std::memcpy( &_data[_size], &v, s );
				
				//char * p = (char*) &v;
				//for ( size_t i = 0; i < s; i++){
					//_data[_size + i] = p[i];
				//}

				_size += s;
			}
			template <typename T>
			void writePos(const T &v, size_t s, size_t pos){
				grow(pos + s);
				std::memcpy( &_data[pos], (char*) &v, s );
				//char * p = (char*) &v;
				//for ( size_t i = 0; i < s; i++){
					//_data[_size + i] = p[i];
				//}

				if(_size < pos+s)
					_size += pos+s;
			}
			template <typename T>
			void writePos(const T &v, size_t pos){
				writePos(v, sizeof(v), pos);
			}

			template <typename T>
			inline void writeSafe(T * v, size_t s){
				std::memcpy( &_data[_size], v, s );
				//char * p = (char*) &v;
				//for ( size_t i = 0; i < s; i++){
				//	_data[_size + i] = p[i];
				//}

				_size += s;
			}
			template <typename T>
			inline void write(T * v, size_t s){
				grow(_size + s);
				writeSafe(v, s);
			}
			/*void write(void * v, size_t s){
				std::memcpy( &_data[_size], v, s );
				_size += s;
			}// */

			template <typename T>
			void write(T v){
				write( v, sizeof(T) );
			}
			void write(std::string i){
				size_t s = i.length();
				grow(_size + sizeof(size_t) + s );
				writeSafe( &s, sizeof(size_t) );
				writeSafe( i.data(), s );
			}
			template <typename T>
			void write(std::vector<T> v){
				size_t s = v.size();
				grow(_size + sizeof(size_t) + s * sizeof(T));
				writeSafe( &s, sizeof(size_t) );
				writeSafe( v.data(), s * sizeof(T) );
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
				std::memcpy(&v, &_data[_size], s );
				//char * p = (char*) &v;
				//for ( size_t i = 0; i < s; i++){
					//p[i] = _data[_size + i];
				//}
				_size += s;
			}
			template <typename T>
			void read(T * v, size_t s){
				std::memcpy(v, &_data[_size], s );
				//char * p = (char*) &v;
				//for ( size_t i = 0; i < s; i++){
					//p[i] = _data[_size + i];
				//}
				_size += s;
			}
			template <typename T>
			void read(T & v){
				read( &v, sizeof(T) );
			}
			template <typename T>
			void readVec(std::vector<T> & v, size_t s){
				v.assign((T*) &_data[_size], (T*) &_data[_size + s] );
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
				readVec(v, size*sizeof(T));
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
}

#endif
