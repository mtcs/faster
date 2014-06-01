#ifndef LIBFASTER_CONTEXT_H
#define LIBFASTER_CONTEXT_H

#include <string>

class fastContext{
	public:
		fastContext(std::string master);
		int createFDD(char);
		void destroyFDD(unsigned long int);
	private:
		int connection;
		int status;
};

#endif
