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

//'lzcm' (Lempel Ziv Codec Module)
//Lempel Ziv - codec

#include <inttypes.h>
#include "lempelziv.h"

// functions:
// 
// 	ret = lzcm_encode(bits,inputptr,inputlen,outputptr,*outputlen)
// 	bits (2-8) - number of bits for each input code (f.e. 8 for bytes)
// 	inputptr - pointer to input buffer (each input code stored in byte)
// 	inputlen - number of bytes to compress
// 	outputptr - pointer to output buffer
// 	outputlen - length in bytes of output buffer (after return: bytes stored in output buffer)
// 	ret - 0 - error, 1 - OK
//
// 	lzcm_decode(bits,inputptr,inputlen,outputptr,*outputlen)
// 	bits (2-8) - number of bits for each output code
// 	inputptr - pointer to input buffer
// 	inputlen - number of bytes in input buffer
// 	outputptr - pointer to output buffer
// 	outputlen - length in bytes of output buffer (after return: bytes stored in output buffer)
// 	ret - 0 - error, 1 - OK
//
//  2005.01.24 - poprawiony b³±d dekompresji - dodanie raportowanie b³êdnych sytuacji

#define HSIZE 4999
#define BITS 12
#define CODES 4096
//#define HASHFUN(ch,code) ((ch)<<4 ^ (code))

//universal data
static uint8_t codebits;			// bits per code (in uncompresed string)
static uint32_t clearcode;			// clear code (2**codebits)
static uint32_t eofcode;			// EOF code (clearcode+1)
static uint32_t firstfree;			// first free code (eofcode+1)
static uint8_t compbits;			// bits of actual code (initially codebits+1)
static uint32_t lastcode;			// code on 'compbits' should increase

//for encoder
static uint32_t tabcode[HSIZE];
static uint32_t tabhash[HSIZE];

//for decoder
static uint32_t prefix[CODES];
static uint8_t suffix[CODES];

//fast stack
static uint32_t stack[CODES];
static uint32_t stackp;
#define push(x) stack[stackp++]=x
#define pop() ((stackp==0)?0xffffffff:stack[--stackp])

//input/output
static uint8_t *winp;
static uint32_t winpl;
static uint32_t wloaded;
static uint8_t *wout;
static uint32_t woutl;
static uint32_t wstored;

//bits operation
static uint32_t bytebuff;
static uint8_t bitsin;
static uint32_t mask[17]={0,1,3,7,15,31,63,127,255,511,1023,2047,4095,8191,16383,32767,65535};

void lzcm_ioinit() {
	bytebuff=0;
	bitsin=0;
}

int lzcm_storebits(uint32_t dat,uint8_t len) {
	bytebuff |= dat<<bitsin;
	bitsin+=len;
	while (bitsin>=8) {
		if (wstored<woutl) {
			*wout++=bytebuff&0xFF;
			wstored++;
		} else {
			return 0;
		}
		bytebuff>>=8;
		bitsin-=8;
	}
	return 1;
}

int lzcm_loadbits(uint32_t *dat,uint8_t len) {
	while (len>bitsin) {
		if (wloaded<winpl) {
			bytebuff|=((uint32_t)(*winp++))<<bitsin;
			wloaded++;
		} else {
			*dat = eofcode;
			return 0;
		}
		bitsin+=8;
	}
	*dat = bytebuff & mask[len];
	bytebuff>>=len;
	bitsin-=len;
	return 1;
}

//load/store codes

int lzcm_storecode(uint32_t code) {
	return lzcm_storebits(code,compbits);
}

int lzcm_loadcode(uint32_t *code) {
	return lzcm_loadbits(code,compbits);
}

//codec
int lzcm_encodestream() {
	uint32_t ent;
	uint32_t c;
	uint32_t code;
	uint32_t hash;
	uint32_t disp;

	if (lzcm_storecode(clearcode)==0) {
		return 0;
	}
	ent = *winp++;
	wloaded++;
	while (wloaded<winpl) {
		c = *winp++;
		wloaded++;
		code = (c<<BITS) + ent;
		hash = code % HSIZE;
		disp = 1+(code & (HSIZE-2));
//		hash = HASHFUN(c,ent);
//		disp = (hash==0)?1:HSIZE - hash;
		while (tabhash[hash]!=0xffffffff && tabhash[hash]!=code) {
//			hash-=disp;
//			if (hash<0) hash+=HSIZE;
			hash+=disp;
			hash%=HSIZE;
		}
		if (tabhash[hash]==code) {
			ent = tabcode[hash];
		} else {
			if (lzcm_storecode(ent)==0) {
				return 0;
			}
			ent = c;
			if (firstfree<CODES) {
				tabcode[hash]=firstfree++;
				tabhash[hash]=code;
				if (firstfree>lastcode) {
					compbits++;
					lastcode<<=1;
				}
			}
		}
	}
	if (lzcm_storecode(ent)==0) {
		return 0;
	}
	if (lzcm_storecode(eofcode)==0) {
		return 0;
	}
	if (lzcm_storebits(0,7)==0) {	//flush buffer
		return 0;
	}
	return 1;
}



int lzcm_decodestream() {
	uint32_t lcode;

	if (lzcm_loadcode(&lcode)==0) {
		return 0;
	}

	while (lcode!=eofcode) {
		if (lcode==clearcode) {
			firstfree = eofcode+1;
			compbits = codebits+1;
			lastcode = clearcode<<1;
		} else {
			if (lcode>=firstfree) {
				return 0;	//bad stream
			}
			if (firstfree<CODES) prefix[firstfree]=lcode;
			while (lcode>=clearcode) {
				push(lcode);
				lcode=prefix[lcode];
			}
			if (firstfree<=CODES) suffix[firstfree-1]=lcode;
			if (wstored<woutl) {
				*wout++=lcode;
				wstored++;
			} else {
				return 0;	//no output space
			}
			while ((lcode=pop())!=0xffffffff) {
				if (wstored<woutl) {
					*wout++=suffix[lcode];
					wstored++;
				} else {
					return 0;	//no output space
				}
			}
			if (firstfree<=CODES) {
				firstfree++;
				if (firstfree>lastcode) {
					compbits++;
					lastcode<<=1;
				}
			}
		}
		if (lzcm_loadcode(&lcode)==0) {
			return 0;
		}
	}
	return 1;
}


int lzw_encode(uint8_t bits,uint8_t *inp,uint32_t inpl,uint8_t *out,uint32_t *outl) {
	uint32_t i;

	winp = (uint8_t *)inp;
	wout = (uint8_t *)out;
	winpl = inpl;
	woutl = *outl;
	wloaded = 0;
	wstored = 0;
	codebits = bits;
	clearcode = 1L<<bits;
	eofcode = clearcode+1;
	firstfree = eofcode+1;
	compbits = codebits+1;
	lastcode = clearcode<<1;

	for (i=0 ; i<HSIZE ; i++) {
		tabhash[i]=0xffffffff;
	}
	lzcm_ioinit();
	if (lzcm_encodestream()==0) {
		*outl=0;
		return 0;
	}
	*outl = wstored;
	return 1;
}

int lzw_decode(uint8_t bits,uint8_t *inp,uint32_t inpl,uint8_t *out,uint32_t *outl) {
	winp = (uint8_t *)inp;
	wout = (uint8_t *)out;
	winpl = inpl;
	woutl = *outl;
	wloaded = 0;
	wstored = 0;
	codebits = bits;
	clearcode = 1L<<bits;
	eofcode = clearcode+1;
	firstfree = eofcode+1;
	compbits = codebits+1;
	lastcode = clearcode<<1;

	stackp=0;
	lzcm_ioinit();
	if (lzcm_decodestream()==0) {
		*outl=0;
		return 0;
	}
	*outl = wstored;
	return 1;
}
