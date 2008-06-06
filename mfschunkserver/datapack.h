/*
   Copyright 2008 Gemius SA.

   This file is part of MooseFS.

   MooseFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   MooseFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with MooseFS.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _DATAPACK_H_
#define _DATAPACK_H_

/* data pack - version 1.1 */

#define CHKPUT64BIT(var,ptr,eptr,error) {                             \
	if ((ptr)+8 > (eptr)) {                                           \
		error;                                                        \
	} else {                                                          \
		(ptr)[0]=((var)>>56)&0xFF;                                    \
		(ptr)[1]=((var)>>48)&0xFF;                                    \
		(ptr)[2]=((var)>>40)&0xFF;                                    \
		(ptr)[3]=((var)>>32)&0xFF;                                    \
		(ptr)[4]=((var)>>24)&0xFF;                                    \
		(ptr)[5]=((var)>>16)&0xFF;                                    \
		(ptr)[6]=((var)>>8)&0xFF;                                     \
		(ptr)[7]=(var)&0xFF;                                          \
		(ptr)+=8;                                                     \
	}                                                                 \
}

#define CHKPUT32BIT(var,ptr,eptr,error) {                             \
	if ((ptr)+4 > (eptr)) {                                           \
		error;                                                        \
	} else {                                                          \
		(ptr)[0]=((var)>>24)&0xFF;                                    \
		(ptr)[1]=((var)>>16)&0xFF;                                    \
		(ptr)[2]=((var)>>8)&0xFF;                                     \
		(ptr)[3]=(var)&0xFF;                                          \
		(ptr)+=4;                                                     \
	}                                                                 \
}

#define CHKPUT16BIT(var,ptr,eptr,error) {                             \
	if ((ptr)+2 > (eptr)) {                                           \
		error;                                                        \
	} else {                                                          \
		(ptr)[0]=((var)>>8)&0xFF;                                     \
		(ptr)[1]=(var)&0xFF;                                          \
		(ptr)+=2;                                                     \
	}                                                                 \
}

#define CHKPUT8BIT(var,ptr,eptr,error) {                              \
	if ((ptr)+1 > (eptr)) {                                           \
		error;                                                        \
	} else {                                                          \
		*(ptr)=(var)&0xFF;                                            \
		(ptr)++;                                                      \
	}                                                                 \
}

#define CHKGET64BIT(var,ptr,eptr,error) {                             \
	if ((ptr)+8 > (eptr)) {                                           \
		error;                                                        \
	} else {                                                          \
		(var)=((ptr)[3]+256*((ptr)[2]+256*((ptr)[1]+256*(ptr)[0])));  \
		(var)<<=32;                                                   \
		(var)|=(((ptr)[7]+256*((ptr)[6]+256*((ptr)[5]+256*(ptr)[4]))))&0xffffffff; \
		(ptr)+=8;                                                     \
	}                                                                 \
}

#define CHKGET32BIT(var,ptr,eptr,error) {                             \
	if ((ptr)+4 > (eptr)) {                                           \
		error;                                                        \
	} else {                                                          \
		(var)=((ptr)[3]+256*((ptr)[2]+256*((ptr)[1]+256*(ptr)[0])));  \
		(ptr)+=4;                                                     \
	}                                                                 \
}

#define CHKGET16BIT(var,ptr,eptr,error) {                             \
	if ((ptr)+2 > (eptr)) {                                           \
		error;                                                        \
	} else {                                                          \
		(var)=(ptr)[1]+256*(ptr)[0];                                  \
		(ptr)+=2;                                                     \
	}                                                                 \
}

#define CHKGET8BIT(var,ptr,eptr,error) {                              \
	if ((ptr)+1 > (eptr)) {                                           \
		error;                                                        \
	} else {                                                          \
		(var)=*(ptr);                                                 \
		(ptr)++;                                                      \
	}                                                                 \
}

#define PUT64BIT(var,ptr) {                                           \
	(ptr)[0]=((var)>>56)&0xFF;                                        \
	(ptr)[1]=((var)>>48)&0xFF;                                        \
	(ptr)[2]=((var)>>40)&0xFF;                                        \
	(ptr)[3]=((var)>>32)&0xFF;                                        \
	(ptr)[4]=((var)>>24)&0xFF;                                        \
	(ptr)[5]=((var)>>16)&0xFF;                                        \
	(ptr)[6]=((var)>>8)&0xFF;                                         \
	(ptr)[7]=(var)&0xFF;                                              \
	(ptr)+=8;                                                         \
}

#define PUT32BIT(var,ptr) {                                           \
	(ptr)[0]=((var)>>24)&0xFF;                                        \
	(ptr)[1]=((var)>>16)&0xFF;                                        \
	(ptr)[2]=((var)>>8)&0xFF;                                         \
	(ptr)[3]=(var)&0xFF;                                              \
	(ptr)+=4;                                                         \
}

#define PUT16BIT(var,ptr) {                                           \
	(ptr)[0]=((var)>>8)&0xFF;                                         \
	(ptr)[1]=(var)&0xFF;                                              \
	(ptr)+=2;                                                         \
}

#define PUT8BIT(var,ptr) {                                            \
	*(ptr)=(var)&0xFF;                                                \
	(ptr)++;                                                          \
}

#define GET64BIT(var,ptr) {                                           \
	(var)=((ptr)[3]+256*((ptr)[2]+256*((ptr)[1]+256*(ptr)[0])));      \
	(var)<<=32;                                                       \
	(var)|=(((ptr)[7]+256*((ptr)[6]+256*((ptr)[5]+256*(ptr)[4]))))&0xffffffff;  \
	(ptr)+=8;                                                         \
}

#define GET32BIT(var,ptr) {                                           \
	(var)=((ptr)[3]+256*((ptr)[2]+256*((ptr)[1]+256*(ptr)[0])));      \
	(ptr)+=4;                                                         \
}

#define GET16BIT(var,ptr) {                                           \
	(var)=(ptr)[1]+256*(ptr)[0];                                      \
	(ptr)+=2;                                                         \
}

#define GET8BIT(var,ptr) {                                            \
	(var)=*(ptr);                                                     \
	(ptr)++;                                                          \
}

#define IPUT64BIT(var,ptr) {                                           \
	(ptr)[7]=((var)>>56)&0xFF;                                        \
	(ptr)[6]=((var)>>48)&0xFF;                                        \
	(ptr)[5]=((var)>>40)&0xFF;                                        \
	(ptr)[4]=((var)>>32)&0xFF;                                        \
	(ptr)[3]=((var)>>24)&0xFF;                                        \
	(ptr)[2]=((var)>>16)&0xFF;                                        \
	(ptr)[1]=((var)>>8)&0xFF;                                         \
	(ptr)[0]=(var)&0xFF;                                              \
	(ptr)+=8;                                                         \
}

#define IPUT32BIT(var,ptr) {                                           \
	(ptr)[3]=((var)>>24)&0xFF;                                        \
	(ptr)[2]=((var)>>16)&0xFF;                                        \
	(ptr)[1]=((var)>>8)&0xFF;                                         \
	(ptr)[0]=(var)&0xFF;                                              \
	(ptr)+=4;                                                         \
}

#define IPUT16BIT(var,ptr) {                                           \
	(ptr)[1]=((var)>>8)&0xFF;                                         \
	(ptr)[0]=(var)&0xFF;                                              \
	(ptr)+=2;                                                         \
}

#define IPUT8BIT(var,ptr) {                                            \
	*(ptr)=(var)&0xFF;                                                \
	(ptr)++;                                                          \
}

#define IGET64BIT(var,ptr) {                                           \
	(var)=((ptr)[4]+256*((ptr)[5]+256*((ptr)[6]+256*(ptr)[7])));      \
	(var)<<=32;                                                       \
	(var)|=(((ptr)[0]+256*((ptr)[1]+256*((ptr)[2]+256*(ptr)[3]))))&0xffffffff;  \
	(ptr)+=8;                                                         \
}

#define IGET32BIT(var,ptr) {                                           \
	(var)=((ptr)[0]+256*((ptr)[1]+256*((ptr)[2]+256*(ptr)[3])));      \
	(ptr)+=4;                                                         \
}

#define IGET16BIT(var,ptr) {                                           \
	(var)=(ptr)[0]+256*(ptr)[1];                                      \
	(ptr)+=2;                                                         \
}

#define IGET8BIT(var,ptr) {                                            \
	(var)=*(ptr);                                                     \
	(ptr)++;                                                          \
}

#define STRLENG(var,ptr,eptr) {                                                       \
	char *__define_strleng_ptr__;                                                     \
	(var)=0;                                                                          \
	__define_strleng_ptr__=(ptr);                                                     \
	while ((char*)__define_strleng_ptr__<(char*)(eptr) && *__define_strleng_ptr__) {  \
		__define_strleng_ptr__++;                                                     \
		(var)++;                                                                      \
	}                                                                                 \
}

#endif
