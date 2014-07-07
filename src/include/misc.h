#ifndef LIBFASTER_MISC_H
#define LIBFASTER_MISC_H

#define FDD_TYPE_NULL 	0x00
#define FDD_TYPE_INT 	0x01

#include <typeinfo>
#include <string>

#include "definitions.h"

fddType decodeType(size_t typeCode);

#endif
