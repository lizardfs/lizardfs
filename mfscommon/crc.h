/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA.

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

//2013/08/31 witekj@op.pl Introduction of the SSE4

#ifndef _CRC_H_
#define _CRC_H_
#include <inttypes.h>
#include <crc_sse4.h>

#define CRC_HARDWARE_SUPPORT_ON() 0x0001 /*0x0000*/
#define CRC_COMPARATIVE_TEST()    0x0000 /*0x0002*/

uint32_t mycrc32( uint32_t crc, const uint8_t *block, uint32_t leng );
uint32_t mycrc32_combine( uint32_t crc1, uint32_t crc2, uint32_t leng2 );
#define mycrc32_zeroblock(crc,zeros) mycrc32_combine((crc)^0xFFFFFFFF,0xFFFFFFFF,(zeros))
#define mycrc32_zeroexpanded(crc,block,leng,zeros) mycrc32_zeroblock(mycrc32((crc),(block),(leng)),(zeros))
#define mycrc32_xorblocks(crc,crcblock1,crcblock2,leng) ((crcblock1)^(crcblock2)^mycrc32_zeroblock(crc,leng))


#if CRC_COMPARATIVE_TEST()
#include <boost/preprocessor.hpp>
#if !CRC_HARDWARE_SUPPORT_ON()
#define CRC_HARDWARE_SUPPORT_ON() 0x0001
#endif

uint32_t BOOST_PP_CAT( sse4_, mycrc32(uint32_t crc,const uint8_t *block,uint32_t leng) );
uint32_t BOOST_PP_CAT( sse4_, mycrc32_combine(uint32_t crc1, uint32_t crc2, uint32_t leng2) );

#endif

#if !CRC_HARDWARE_SUPPORT_ON() | CRC_COMPARATIVE_TEST()
void mycrc32_init(void);
#endif

#endif
