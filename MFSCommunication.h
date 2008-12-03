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

#ifndef _MFS_COMMUNICATION_H_
#define _MFS_COMMUNICATION_H_

// all data field transfered in network order.
// packet structure:
// type:32 length:32 data:lengthB
//


//UNIVERSAL
#define VERSION_ANY 0

#define CRC_POLY 0xEDB88320U

#define MFS_ROOT_ID 1

#define STATUS_OK 0

/* master errors */
//#define ERROR_BADID 1				/* podany identyfikator nie istnieje */
//#define ERROR_IDNOTDIR 2			/* podany identyfikator nie wskazuje na katalog */
//#define ERROR_NOTDIR 3				/* podany identyfikator+nazwa nie wskazuje na katalog */
//#define ERROR_NOTFILE 4				/* podany identyfikator+nazwa nie wskazuje na plik */
//#define ERROR_EXIST 5				/* element ju¿ istnieje */
//#define ERROR_NOENT 6				/* nie ma takiego elementu */
//#define ERROR_NOTEMPTY 7			/* katalog nie jest pusty */
//#define ERROR_MOVETODESCENDANT 8	/* b³êdny rename - próba umieszczenia katalogu w swoim katalogu potomnym */
//#define ERROR_INVALIDNAME 9			/* nazwa jest b³êdna ( pusta lub zawiera znaki '/' ) */

#define ERROR_EPERM 1	// new (old: ERROR_NOTFILE itp. )
#define ERROR_ENOTDIR 2	// old: ERROR_IDNOTDIR + ERROR_NOTDIR
#define ERROR_ENOENT 3	// old: ERROR_NOENT + ERROR_BADID
#define ERROR_EACCES 4	// old: ERROR_NOACCESS
#define ERROR_EEXIST 5	// old: ERROR_EXIST
#define ERROR_EINVAL 6	// old: ERROR_MOVETODESCENDANT + ERROR_INVALIDNAME + ERROR_NOTSYMLINK
#define ERROR_ENOTEMPTY 7	// old: ERROR_NOTEMPTY
#define ERROR_CHUNKLOST 8			/* przy zapisie okaza³o siê, ¿e nie ma ani jednej dobrej kopii chunka - trzeba reinicjalizowaæ */
#define ERROR_OUTOFMEMORY 9		/* skoñczy³a siê pamiêæ */

#define ERROR_INDEXTOOBIG 10		/* za du¿y indeks "chunka" */
#define ERROR_LOCKED 11				/* próba ponownego zapisu do tego samego "chunka" */
#define ERROR_NOCHUNKSERVERS 12		/* nie mozna utworzyc nowego chunka bo nie ma serwerow */
#define ERROR_NOCHUNK 13			/* podany chunk nie istnieje */
#define ERROR_CHUNKBUSY	14			/* nie mozna wykonac operacji bo chunk jest w trakcie jakiejs operacji */
#define ERROR_REGISTER 15			/* nieprawid³owy BLOB podany przy rejestracji */
#define ERROR_NOTDONE 16			/* zwracany po write-chunk - zaden chunk-serwer nie wykonal zleconej operacji (create/duplicate/setversion) */
#define ERROR_NOTOPENED 17			/* proba wykonania operacji na pliku, ktory nie zostal wczesniej otwarty */
#define ERROR_NOTSTARTED 18			/* write-end bez uprzedniego write */

#define ERROR_WRONGVERSION 19		/* niepoprawna wersja chunk'a */
#define ERROR_CHUNKEXIST 20			/* taki chunk ju¿ istnieje / te¿ próba reinicjalizacji dobrego chunka */
#define ERROR_NOSPACE 21			/* skoñczy³o siê miejsce na dysku */
#define ERROR_IO 22					/* fizyczny b³±d odczytu/zapisu na dysku */
#define ERROR_BNUMTOOBIG 23			/* odczyt/zapis - b³êdny numer bloku */
#define ERROR_WRONGSIZE	24			/* odczyt/zapis - b³êdna wielko¶æ bloku danych */
#define ERROR_WRONGOFFSET 25		/* odczyt/zapis - b³êdna pozycja */
#define ERROR_CANTCONNECT 26		/* nie mo¿na po³±czyæ siê z podanym serwerem */
#define ERROR_WRONGCHUNKID 27		/* zwrócono innego chunk'a ni¿ zamawiany */
#define ERROR_DISCONNECTED 28		/* transmisja zosta³a zerwana */
#define ERROR_CRC 29				/* wyst±pi³ b³±d testowania sumy kontrolnej */
#define ERROR_DELAYED 30			/* setattr trzeba opoznic - wewnetrzny */
#define ERROR_CANTCREATEPATH 31		/* nie moge utworzyc sciezki podczas undelete */

#define ERROR_MISMATCH 32		/* niezgodno¶æ podczas odtwarzania metadanych z logu */

/* type for readdir command */
#define TYPE_FILE 'f'
#define TYPE_SYMLINK 'l'
#define TYPE_DIRECTORY 'd'
#define TYPE_FIFO 'q'
#define TYPE_BLOCKDEV 'b'
#define TYPE_CHARDEV 'c'
#define TYPE_SOCKET 's'
// 't' and 'r' are only for internal master use - they are in readdir shown as 'f'
#define TYPE_TRASH 't'
#define TYPE_RESERVED 'r'
#define TYPE_UNKNOWN '?'

// mode mask:  "modemask" field in "CUTOMA_FUSE_ACCESS"
#define MODE_MASK_R 4
#define MODE_MASK_W 2
#define MODE_MASK_X 1

// flags: "setmask" field in "CUTOMA_FUSE_SETATTR"
// SET_GOAL_FLAG,SET_DELETE_FLAG are no longer supported
// SET_LENGTH_FLAG,SET_OPENED_FLAG are deprecated
// instead of using FUSE_SETATTR with SET_GOAL_FLAG use FUSE_SETGOAL command
// instead of using FUSE_SETATTR with SET_GOAL_FLAG use FUSE_SETTRASH_TIMEOUT command
// instead of using FUSE_SETATTR with SET_LENGTH_FLAG/SET_OPENED_FLAG use FUSE_TRUNCATE command
#define SET_GOAL_FLAG 0x0001
#define SET_MODE_FLAG 0x0002
#define SET_UID_FLAG 0x0004
#define SET_GID_FLAG 0x0008
#define SET_LENGTH_FLAG 0x0010
#define SET_MTIME_FLAG 0x0020
#define SET_ATIME_FLAG 0x0040
#define SET_OPENED_FLAG 0x0080
#define SET_DELETE_FLAG 0x0100

// dtypes:
#define DTYPE_UNKNOWN 0
#define DTYPE_TRASH 1
#define DTYPE_RESERVED 2
#define DTYPE_ISVALID(x) (((uint32_t)(x))<=2)

// smode:
#define SMODE_SET 0
#define SMODE_INCREASE 1
#define SMODE_DECREASE 2
#define SMODE_RSET 4
#define SMODE_RINCREASE 5
#define SMODE_RDECREASE 6
#define SMODE_TMASK 3
#define SMODE_RMASK 4
#define SMODE_ISVALID(x) (((x)&SMODE_TMASK)!=3 && ((uint32_t)(x))<=7)

// gmode:
#define GMODE_NORMAL 0
#define GMODE_RECURSIVE 1
#define GMODE_ISVALID(x) (((uint32_t)(x))<=1)

// flags: "flags" fileld in "CUTOMA_FUSE_AQUIRE"
#define WANT_READ 1
#define WANT_WRITE 2
#define AFTER_CREATE 4

#define ERROR_STRINGS \
	"OK", \
	"Operation not permitted", \
	"Not a directory", \
	"No such file or directory", \
	"Permission denied", \
	"File exists", \
	"Invalid argument", \
	"Directory not empty", \
	"Chunk lost", \
	"Out of memory", \
	"Index too big", \
	"Chunk locked", \
	"No chunk servers", \
	"No such chunk", \
	"Chunk is busy", \
	"Incorrect register BLOB", \
	"None of chunk servers performed requested operation", \
	"File not opened", \
	"Write not started", \
	"Wrong chunk version", \
	"Chunk already exists", \
	"No space left", \
	"IO error", \
	"Incorrect block number", \
	"Incorrect size", \
	"Incorrect offset", \
	"Can't connect", \
	"Incorrect chunk id", \
	"Disconnected", \
	"CRC error", \
	"Operation delayed", \
	"Can't create path", \
	"Data mismatch", \
	""

//ANY <-> ANY

#define ANTOAN_NOP 0

//CHUNKSERVER <-> MASTER

#define CSTOMA_REGISTER 100
// myip:32 myport:16 usedspace:64 totalspace:64 N*[ chunkid:64 version:32 ]
// rver:8
// 	rver==1:
// 		myip:32 myport:16 usedspace:64 totalspace:64 tdusedspace:64 tdtotalspace:64 tdchunks:32 N*[ chunkid:64 version:32 ]
// 	rver==2:
// 		myip:32 myport:16 usedspace:64 totalspace:64 chunks:32 tdusedspace:64 tdtotalspace:64 tdchunks:32 N*[ chunkid:64 version:32 ]
#define CSTOMA_SPACE 101
// usedspace:64 totalspace:64
// usedspace:64 totalspace:64 tdusedspace:64 tdtotalspace:64
// usedspace:64 totalspace:64 chunks:32 tdusedspace:64 tdtotalspace:64 tdchunks:32
#define CSTOMA_CHUNK_DAMAGED 102
// N*[chunkid:64]	- now N is always 1
#define MATOCS_STRUCTURE_LOG 103
// version:32 logdata:string ( N*[ char:8 ] )
#define MATOCS_STRUCTURE_LOG_ROTATE 104
// -
#define CSTOMA_CHUNK_LOST 105
// N*[chunkid:64]	- now N is always 1
#define CSTOMA_ERROR_OCCURRED 106
// -

#define MATOCS_CREATE 110
// chunkid:64 version:32
#define CSTOMA_CREATE 111
// chunkid:64 status:8

#define MATOCS_DELETE 120
// chunkid:64 version:32
#define CSTOMA_DELETE 121
// chunkid:64 status:8

#define MATOCS_DUPLICATE 130
// chunkid:64 version:32 oldchunkid:64 [newversion:32] oldversion:32
#define CSTOMA_DUPLICATE 131
// chunkid:64 status:8

#define MATOCS_SET_VERSION 140
// chunkid:64 version:32 oldversion:32
#define CSTOMA_SET_VERSION 141
// chunkid:64 status:8

#define MATOCS_REPLICATE 150
// chunkid:64 version:32 ip:32 port:16
#define CSTOMA_REPLICATE 151
// chunkid:64 version:32 status:8

#define MATOCS_TRUNCATE 160
// chunkid:64 length:32 version:32 oldversion:32 
#define CSTOMA_TRUNCATE 161
// chunkid:64 status:8

#define MATOCS_DUPTRUNC 170
// chunkid:64 version:32 oldchunkid:64 [newversion:32] oldversion:32 length:32
#define CSTOMA_DUPTRUNC 171
// chunkid:64 status:8

// New idea:
//
// #define MATOCS_CHUNK_OPERATION 180
// copychunkdid:64 copyversion:32 copylength:32 originalchunkdid:64 originalnewversion:32 originalversion:32
// NID NCV   0   0   0   0 / NID STATUS - CREATE
//   0   0   0 CID   0 OVE / CID STATUS - DELETE
// NID NCV MAX CID NVE OVE / CID STATUS - DUPLICATE
//   0   0 MAX CID NVE OVE / CID STATUS - SET_VERSION
//   0   0 LEN CID NVE OVE / CID STATUS - TRUNCATE
// NID NCV LEN CID NVE OVE / CID STATUS - DUPTRUNC
//
// NID - new chunk id
// NCV - new chunk version
// MAX - max chunk length (64MB)
// LEN - new chunk length
// CID - chunk id
// NVE - new version
// OVE - old version

//CHUNKSERVER <-> CUSTOMER/CHUNKSERVER

#define CUTOCS_READ 200
// chunkid:64 version:32 offset:32 size:32
#define CSTOCU_READ_STATUS 201
// chunkid:64 status:8
#define CSTOCU_READ_DATA 202
// chunkid:64 blocknum:16 offset:16 size:32 crc:32 size*[ databyte:8 ]

#define CUTOCS_WRITE 210
// chunkid:64 version:32 N*[ ip:32 port:16 ]
#define CSTOCU_WRITE_STATUS 211
// chunkid:64 writeid:32 status:8
#define CUTOCS_WRITE_DATA 212
// chunkid:64 writeid:32 blocknum:16 offset:16 size:32 crc:32 size*[ databyte:8 ]

// #define CUTOCS_WRITE_DONE 212
// chunkid:64

//CHUNKSERVER <-> CHUNKSERVER

#define CSTOCS_GET_CHUNK_BLOCKS 250
// chunkid:64 version:32
#define CSTOCS_GET_CHUNK_BLOCKS_STATUS 251
// chunkid:64 version:32 blocks:16 status:8

//ANY <-> CHUNKSERVER

#define ANTOCS_CHUNK_CHECKSUM 300
// chunkid:64 version:32
#define CSTOAN_CHUNK_CHECKSUM 301
// chunkid:64 version:32 checksum:32
// chunkid:64 version:32 status:8

#define ANTOCS_CHUNK_CHECKSUM_TAB 302
// chunkid:64 version:32
#define CSTOAN_CHUNK_CHECKSUM_TAB 303
// chunkid:64 version:32 1024*[checksum:32]
// chunkid:64 version:32 status:8

//CUSTOMER <-> MASTER

#define FUSE_REGISTER_BLOB "kFh9mdZsR84l5e675v8bi54VfXaXSYozaU3DSz9AsLLtOtKipzb9aQNkxeOISx62"
#define FUSE_REGISTER_BLOB_NOPS "kFh9mdZsR84l5e675v8bi54VfXaXSYozaU3DSz9AsLLtOtKipzb9aQNkxeOISx63"
#define FUSE_REGISTER_BLOB_DNAMES "kFh9mdZsR84l5e675v8bi54VfXaXSYozaU3DSz9AsLLtOtKipzb9aQNkxeOISx64"

// old attr record:
//   type:8 flags:8 mode:16 uid:32 gid:32 atime:32 mtime:32 ctime:32 length:64
//   total: 32B (1+1+2+4+4+4+4+4+8)
//
//   flags: ---DGGGG
//             |\--/
//             |  \------ goal
//             \--------- delete imediatelly

// new attr record:
//   type:8 mode:16 uid:32 gid:32 atime:32 mtime:32 ctime:32 nlink:32 length:64
//   total: 35B
//
//   in case of BLOCKDEV and CHARDEV instead of 'length:64' on the end there is 'mojor:16 minor:16 empty:32'


// NAME type:
// ( leng:8 data:lengB )

// clientid==0 means "new client"
#define CUTOMA_FUSE_REGISTER 400
// blob:64B [clientid:32 [version:32]]
#define MATOCU_FUSE_REGISTER 401
// status:8
// clientid:32
#define CUTOMA_FUSE_STATFS 402
// msgid:32 - 
#define MATOCU_FUSE_STATFS 403
// msgid:32 totalspace:64 availspace:64 trashspace:64 inodes:32
#define CUTOMA_FUSE_ACCESS 404
// msgid:32 inode:32 uid:32 gid:32 modemask:8
#define MATOCU_FUSE_ACCESS 405
// msgid:32 status:8
#define CUTOMA_FUSE_LOOKUP 406
// msgid:32 inode:32 name:NAME uid:32 gid:32
#define MATOCU_FUSE_LOOKUP 407
// msgid:32 status:8
// msgid:32 inode:32 attr:35B
#define CUTOMA_FUSE_GETATTR 408
// msgid:32 inode:32
#define MATOCU_FUSE_GETATTR 409
// msgid:32 status:8
// msgid:32 attr:35B
#define CUTOMA_FUSE_SETATTR 410
// msgid:32 inode:32 uid:32 gid:32 setmask:8 attr:32B	- compatibility with very old version
// msgid:32 inode:32 uid:32 gid:32 setmask:16 attr:32B  - compatibility with old version
// msgid:32 inode:32 uid:32 gid:32 setmask:8 attrmode:16 attruid:32 attrgid:32 attratime:32 attrmtime:32
#define MATOCU_FUSE_SETATTR 411
// msgid:32 status:8
// msgid:32 attr:35B
#define CUTOMA_FUSE_READLINK 412
// msgid:32 inode:32
#define MATOCU_FUSE_READLINK 413
// msgid:32 status:8
// msgid:32 length:32 path:lengthB
#define CUTOMA_FUSE_SYMLINK 414
// msgid:32 inode:32 name:NAME length:32 path:lengthB uid:32 gid:32
#define MATOCU_FUSE_SYMLINK 415
// msgid:32 status:8
// msgid:32 inode:32 attr:35B
#define CUTOMA_FUSE_MKNOD 416
// msgid:32 inode:32 name:NAME type:8 mode:16 uid:32 gid:32 rdev:32
#define MATOCU_FUSE_MKNOD 417
// msgid:32 status:8
// msgid:32 inode:32 attr:35B 
#define CUTOMA_FUSE_MKDIR 418
// msgid:32 inode:32 name:NAME mode:16 uid:32 gid:32
#define MATOCU_FUSE_MKDIR 419
// msgid:32 status:8
// msgid:32 inode:32 attr:35B
#define CUTOMA_FUSE_UNLINK 420
// msgid:32 inode:32 name:NAME uid:32 gid:32
#define MATOCU_FUSE_UNLINK 421
// msgid:32 status:8
#define CUTOMA_FUSE_RMDIR 422
// msgid:32 inode:32 name:NAME uid:32 gid:32
#define MATOCU_FUSE_RMDIR 423
// msgid:32 status:8
#define CUTOMA_FUSE_RENAME 424
// msgid:32 inode_src:32 name_src:NAME inode_dst:32 name_dst:NAME uid:32 gid:32
#define MATOCU_FUSE_RENAME 425
// msgid:32 status:8
#define CUTOMA_FUSE_LINK 426
// msgid:32 inode:32 inode_dst:32 name_dst:NAME uid:32 gid:32
#define MATOCU_FUSE_LINK 427
// msgid:32 status:8
// msgid:32 inode:32 attr:35B
#define CUTOMA_FUSE_GETDIR 428
// msgid:32 inode:32 uid:32 gid:32
#define MATOCU_FUSE_GETDIR 429
// msgid:32 status:8
// msgid:32 N*[ name:NAME inode:32 type:8 ]

#define CUTOMA_FUSE_OPEN 430
// msgid:32 inode:32 uid:32 gid:32 flags:8
#define MATOCU_FUSE_OPEN 431
// msgid:32 status:8

#define CUTOMA_FUSE_READ_CHUNK 432
// msgid:32 inode:32 chunkindx:32
#define MATOCU_FUSE_READ_CHUNK 433
// msgid:32 status:8
// msgid:32 length:64 chunkid:64 version:32 N*[ip:32 port:16]
#define CUTOMA_FUSE_WRITE_CHUNK 434 /* it creates, duplicates or sets new version of chunk if necessary */
// msgid:32 inode:32 chunkindx:32
#define MATOCU_FUSE_WRITE_CHUNK 435
// msgid:32 status:8
// msgid:32 length:64 chunkid:64 version:32 N*[ip:32 port:16]
#define CUTOMA_FUSE_WRITE_CHUNK_END 436
// msgid:32 chunkid:64 inode:32 length:64
#define MATOCU_FUSE_WRITE_CHUNK_END 437
// msgid:32 status:8


#define CUTOMA_FUSE_APPEND 438
// msgid:32 inode:32 srcinode:32 uid:32 gid:32
#define MATOCU_FUSE_APPEND 439
// msgid:32 status:8


#define CUTOMA_FUSE_CHECK 440
// msgid:32 inode:32
#define MATOCU_FUSE_CHECK 441
// msgid:32 status:8
// msgid:32 N*[ copies:8 chunks:16 ]

#define CUTOMA_FUSE_GETTRASHTIME 442
// msgid:32 inode:32 gmode:8
#define MATOCU_FUSE_GETTRASHTIME 443
// msgid:32 status:8
// msgid:32 tdirs:32 tfiles:32 tdirs*[ trashtime:32 dirs:32 ] tfiles*[ trashtime:32 files:32 ]

#define CUTOMA_FUSE_SETTRASHTIME 444
// msgid:32 inode:32 uid:32 trashtimeout:32 smode:8
#define MATOCU_FUSE_SETTRASHTIME 445
// msgid:32 status:8
// msgid:32 changed:32 notchanged:32 notpermitted:32

#define CUTOMA_FUSE_GETGOAL 446
// msgid:32 inode:32 gmode:8
#define MATOCU_FUSE_GETGOAL 447
// msgid:32 status:8
// msgid:32 gdirs:8 gfiles:8 gdirs*[ goal:8 dirs:32 ] gfiles*[ goal:8 files:32 ]

#define CUTOMA_FUSE_SETGOAL 448
// msgid:32 inode:32 uid:32 goal:8 smode:8
#define MATOCU_FUSE_SETGOAL 449
// msgid:32 status:8
// msgid:32 changed:32 notchanged:32 notpermitted:32

#define CUTOMA_FUSE_GETTRASH 450
// msgid:32
#define MATOCU_FUSE_GETTRASH 451
// msgid:32 N*[ name:NAME inode:32 ]

#define CUTOMA_FUSE_GETDETACHEDATTR 452
// msgid:32 inode:32 dtype:8
#define MATOCU_FUSE_GETDETACHEDATTR 453
// msgid:32 status:8
// msgid:32 attr:35B

#define CUTOMA_FUSE_GETTRASHPATH 454
// msgid:32 inode:32
#define MATOCU_FUSE_GETTRASHPATH 455
// msgid:32 status:8
// msgid:32 length:32 path:lengthB

#define CUTOMA_FUSE_SETTRASHPATH 456
// msgid:32 inode:32 length:32 path:lengthB
#define MATOCU_FUSE_SETTRASHPATH 457
// msgid:32 status:8

#define CUTOMA_FUSE_UNDEL 458
// msgid:32 inode:32
#define MATOCU_FUSE_UNDEL 459
// msgid:32 status:8

#define CUTOMA_FUSE_PURGE 460
// msgid:32 inode:32
#define MATOCU_FUSE_PURGE 461
// msgid:32 status:8

#define CUTOMA_FUSE_GETDIRSTATS 462
// msgid:32 inode:32
#define MATOCU_FUSE_GETDIRSTATS 463
// msgid:32 status:8
// msgid:32 inodes:32 dirs:32 files:32 ugfiles:32 mfiles:32 chunks:32 ugchunks:32 mchunks32 length:64 size:64 gsize:64

#define CUTOMA_FUSE_TRUNCATE 464
// msgid:32 inode:32 uid:32 gid:32 opened:8 length:64
#define MATOCU_FUSE_TRUNCATE 465
// msgid:32 status:8
// msgid:32 attr:35B

#define CUTOMA_FUSE_GETRESERVED 470
// msgid:32
#define MATOCU_FUSE_GETRESERVED 471
// msgid:32 N*[ name:NAME inode:32 ]


// special - reserved (opened) inodes - keep opened files.
#define CUTOMA_FUSE_RESERVED_INODES 499
// N*[inode:32]


// CUSTOMER <-> MASTER (stats - unregistered)

#define CUTOMA_CSERV_LIST 500
// -
#define MATOCU_CSERV_LIST 501
// N*[ip:32 port:16 used:64 total:64 chunks:32 tdused:64 tdtotal:64 tdchunks:32 errorcount:32 ]

#define CUTOCS_HDD_LIST 502
// -
#define CSTOCU_HDD_LIST 503
// N*[ path:NAME flags:8 errchunkid:64 errtime:32 used:64 total:64 chunkscount:32 ]

#define CUTOAN_CHART 504
// chartid:32
#define ANTOCU_CHART 505
// chart:GIF

#define CUTOAN_CHART_DATA 506
// chartid:32
#define ANTOCU_CHART_DATA 507
// time:32 N*[ data:64 ]

#define CUTOMA_CUST_LIST 508
// -
#define MATOCU_CUST_LIST 509
// N*[ip:32 version:32]

#define CUTOMA_INFO 510
// -
#define MATOCU_INFO 511
// totalspace:64 availspace:64 trashspace:64 trashnodes:32 reservedspace:64 reservednodes:32 allnodes:32 dirnodes:32 filenodes:32 chunks:32 tdchunks:32

#define CUTOMA_FSTEST_INFO 512
// -
#define MATOCU_FSTEST_INFO 513
// loopstart:32 loopend:32 files:32 ugfiles:32 mfiles:32 chunks:32 ugchunks:32 mchunks:32 msgleng:32 msgleng*[ char:8]

#define CUTOMA_CHUNKSTEST_INFO 514
// -
#define MATOCU_CHUNKSTEST_INFO 515
// loopstart:32 loopend:32 del_invalid:32 nodel_invalid:32 del_unused:32 nodel_unused:32 del_diskclean:32 nodel_diskclean:32 del_overgoal:32 nodel_overgoal:32 copy_undergoal:32 nocopy_undergoal:32 copy_rebalance:32

// #define CUTOAN_STAT_RECORD 502
// feature:32 timeid:8 
// #define ANTOCU_STAT_RECORD 503
// N*[data:64]

// #define CUTOAN_STAT_DATA 504
// feature:32
// #define ANTOCU_STAT_DATA 505
// data:64



// CRC32 - example code:
//
// uint32_t* crc32_generate(void) {
// 	uint32_t *res;
// 	uint32_t crc, poly, i, j;
// 
// 	res = (uint32_t*)malloc(sizeof(uint32_t)*256);
// 	poly = CRC_POLY;
// 	for (i=0 ; i<256 ; i++) {
// 		crc=i;
// 		for (j=0 ; j<8 ; j++) {
// 			if (crc & 1) {
// 				crc = (crc >> 1) ^ poly;
// 			} else {
// 				crc >>= 1;
// 			}
// 		}
// 		res[i] = crc;
// 	}
// 	return res;
// }
// 
// uint32_t crc32(uint32_t crc,uint8_t *block,uint32_t leng) {
// 	uint8_t c;
// 	static uint32_t *crc_table = NULL;
// 
// 	if (crc_table==NULL) {
// 		crc_table = crc32_generate();
// 	}
// 
// 	crc^=0xFFFFFFFF;
// 	while (leng>0) {
// 		c = *block++;
// 		leng--;
// 		crc = ((crc>>8) & 0x00FFFFFF) ^ crc_table[ (crc^c) & 0xFF ];
// 	}
// 	return crc^0xFFFFFFFF;
// }
//

#endif
