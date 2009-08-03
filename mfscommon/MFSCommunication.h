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

#define MFS_NAME_MAX 255
#define MFS_MAX_FILE_SIZE 0x20000000000LL

#define STATUS_OK              0	// OK

#define ERROR_EPERM            1	// Operation not permitted
#define ERROR_ENOTDIR          2	// Not a directory
#define ERROR_ENOENT           3	// No such file or directory
#define ERROR_EACCES           4	// Permission denied
#define ERROR_EEXIST           5	// File exists
#define ERROR_EINVAL           6	// Invalid argument
#define ERROR_ENOTEMPTY        7	// Directory not empty
#define ERROR_CHUNKLOST        8	// Chunk lost
#define ERROR_OUTOFMEMORY      9	// Out of memory

#define ERROR_INDEXTOOBIG     10	// Index too big
#define ERROR_LOCKED          11	// Chunk locked
#define ERROR_NOCHUNKSERVERS  12	// No chunk servers
#define ERROR_NOCHUNK         13	// No such chunk
#define ERROR_CHUNKBUSY	      14	// Chunk is busy
#define ERROR_REGISTER        15	// Incorrect register BLOB
#define ERROR_NOTDONE         16	// None of chunk servers performed requested operation
#define ERROR_NOTOPENED       17	// File not opened
#define ERROR_NOTSTARTED      18	// Write not started

#define ERROR_WRONGVERSION    19	// Wrong chunk version
#define ERROR_CHUNKEXIST      20	// Chunk already exists
#define ERROR_NOSPACE         21	// No space left
#define ERROR_IO              22	// IO error
#define ERROR_BNUMTOOBIG      23	// Incorrect block number
#define ERROR_WRONGSIZE	      24	// Incorrect size
#define ERROR_WRONGOFFSET     25	// Incorrect offset
#define ERROR_CANTCONNECT     26	// Can't connect
#define ERROR_WRONGCHUNKID    27	// Incorrect chunk id
#define ERROR_DISCONNECTED    28	// Disconnected
#define ERROR_CRC             29	// CRC error
#define ERROR_DELAYED         30	// Operation delayed
#define ERROR_CANTCREATEPATH  31	// Can't create path

#define ERROR_MISMATCH        32	// Data mismatch

#define ERROR_EROFS           33	// Read-only file system
#define ERROR_QUOTA           34	// Quota exceeded
#define ERROR_BADSESSIONID    35	// Bad session id
#define ERROR_NOPASSWORD      36        // Password is needed
#define ERROR_BADPASSWORD     37        // Incorrect password

#define ERROR_MAX             38

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
	"Read-only file system", \
	"Quota exceeded", \
	"Bad session id", \
	"Password is needed", \
	"Incorrect password", \
	"Unknown MFS error"

/* type for readdir command */
#define TYPE_FILE             'f'
#define TYPE_SYMLINK          'l'
#define TYPE_DIRECTORY        'd'
#define TYPE_FIFO             'q'
#define TYPE_BLOCKDEV         'b'
#define TYPE_CHARDEV          'c'
#define TYPE_SOCKET           's'
// 't' and 'r' are only for internal master use - they are in readdir shown as 'f'
#define TYPE_TRASH            't'
#define TYPE_RESERVED         'r'
#define TYPE_UNKNOWN          '?'

// mode mask:  "modemask" field in "CUTOMA_FUSE_ACCESS"
#define MODE_MASK_R            4
#define MODE_MASK_W            2
#define MODE_MASK_X            1

// flags: "setmask" field in "CUTOMA_FUSE_SETATTR"
// SET_GOAL_FLAG,SET_DELETE_FLAG are no longer supported
// SET_LENGTH_FLAG,SET_OPENED_FLAG are deprecated
// instead of using FUSE_SETATTR with SET_GOAL_FLAG use FUSE_SETGOAL command
// instead of using FUSE_SETATTR with SET_GOAL_FLAG use FUSE_SETTRASH_TIMEOUT command
// instead of using FUSE_SETATTR with SET_LENGTH_FLAG/SET_OPENED_FLAG use FUSE_TRUNCATE command
#define SET_GOAL_FLAG          0x0001
#define SET_MODE_FLAG          0x0002
#define SET_UID_FLAG           0x0004
#define SET_GID_FLAG           0x0008
#define SET_LENGTH_FLAG        0x0010
#define SET_MTIME_FLAG         0x0020
#define SET_ATIME_FLAG         0x0040
#define SET_OPENED_FLAG        0x0080
#define SET_DELETE_FLAG        0x0100

// dtypes:
#define DTYPE_UNKNOWN          0
#define DTYPE_TRASH            1
#define DTYPE_RESERVED         2
#define DTYPE_ISVALID(x)       (((uint32_t)(x))<=2)

// smode:
#define SMODE_SET              0
#define SMODE_INCREASE         1
#define SMODE_DECREASE         2
#define SMODE_RSET             4
#define SMODE_RINCREASE        5
#define SMODE_RDECREASE        6
#define SMODE_TMASK            3
#define SMODE_RMASK            4
#define SMODE_ISVALID(x)       (((x)&SMODE_TMASK)!=3 && ((uint32_t)(x))<=7)

// gmode:
#define GMODE_NORMAL           0
#define GMODE_RECURSIVE        1
#define GMODE_ISVALID(x)       (((uint32_t)(x))<=1)

// extraattr:

#define EATTR_BITS             4

#define EATTR_NOOWNER          0x01
#define EATTR_NOACACHE         0x02
#define EATTR_NOECACHE         0x04
#define EATTR_UNDEFINED        0x08

#define EATTR_STRINGS \
	"noowner", \
	"noattrcache", \
	"noentrycache", \
	""

#define EATTR_DESCRIPTIONS \
	"every user (except root) sees object as his (her) own", \
	"prevent standard object attributes from being stored in kernel cache", \
	"prevent directory entries from being stored in kernel cache", \
	"(not defined)"

// mode attr (higher 4 bits of mode in node attr)
#define MATTR_NOACACHE         0x01
#define MATTR_NOECACHE         0x02
#define MATTR_UNDEFINED_1      0x04
#define MATTR_UNDEFINED_2      0x08

// quota:
#define QUOTA_FLAG_SINODES     0x01
#define QUOTA_FLAG_SLENGTH     0x02
#define QUOTA_FLAG_SSIZE       0x04
#define QUOTA_FLAG_SREALSIZE   0x08
#define QUOTA_FLAG_SALL        0x0F
#define QUOTA_FLAG_HINODES     0x10
#define QUOTA_FLAG_HLENGTH     0x20
#define QUOTA_FLAG_HSIZE       0x40
#define QUOTA_FLAG_HREALSIZE   0x80
#define QUOTA_FLAG_HALL        0xF0

// getdir:
#define GETDIR_FLAG_WITHATTR   0x01

// register sesflags:
#define SESFLAG_READONLY       0x01	// meaning is obvious
#define SESFLAG_DYNAMICIP      0x02	// sessionid can be used by any IP - dangerous for high privileged sessions - one could connect from different computer using stolen session id
#define SESFLAG_IGNOREGID      0x04	// gid is ignored during access testing (when user id is different from object's uid then or'ed 'group' and 'other' rights are used)
#define SESFLAG_CANCHANGEQUOTA 0x08	// quota can be set and deleted
#define SESFLAG_MAPALL         0x10	// all users (except root) are mapped to specific uid and gid

#define SESFLAG_POS_STRINGS \
	"read-only", \
	"not_restricted_ip", \
	"ignore_gid", \
	"can_change_quota", \
	"map_all", \
	"undefined_flag_5", \
	"undefined_flag_6", \
	"undefined_flag_7"

#define SESFLAG_NEG_STRINGS \
	"read-write", \
	"restricted_ip", \
	NULL, \
	NULL, \
	NULL, \
	NULL, \
	NULL, \
	NULL

// flags: "flags" fileld in "CUTOMA_FUSE_AQUIRE"
#define WANT_READ 1
#define WANT_WRITE 2
#define AFTER_CREATE 4

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
// 	rver==3:
// 		myip:32 myport:16 tpctimeout:16 usedspace:64 totalspace:64 chunks:32 tdusedspace:64 tdtotalspace:64 tdchunks:32 N*[ chunkid:64 version:32 ]
// 	rver==4:
// 		version:32 myip:32 myport:16 tcptimeout:16 usedspace:64 totalspace:64 chunks:32 tdusedspace:64 tdtotalspace:64 tdchunks:32 N*[ chunkid:64 version:32 ]
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
// chunkid:64 version:32 oldchunkid:64 oldversion:32
#define CSTOMA_DUPLICATE 131
// chunkid:64 status:8

#define MATOCS_SET_VERSION 140
// chunkid:64 version:32 oldversion:32
#define CSTOMA_SET_VERSION 141
// chunkid:64 status:8

#define MATOCS_REPLICATE 150
// simple copy:
//  chunkid:64 version:32 ip:32 port:16
// multi copy (make new chunk as XOR of couple of chunks)
//  chunkid:64 version:32 N*[chunkid:64 version:32 ip:32 port:16]
#define CSTOMA_REPLICATE 151
// chunkid:64 version:32 status:8

#define MATOCS_CHUNKOP 152
// all chunk operations
// newversion>0 && length==0xFFFFFFFF && copychunkid==0              -> change version
// newversion>0 && length==0xFFFFFFFF && copycnunkid>0               -> duplicate
// newversion>0 && length>=0 && length<=0x4000000 && copychunkid==0  -> truncate
// newversion>0 && length>=0 && length<=0x4000000 && copychunkid>0   -> duplicate and truncate
// newversion==0 && length==0                                        -> delete
// newversion==0 && length==1                                        -> create
// newversion==0 && length==2                                        -> test
// chunkid:64 version:32 newversion:32 copychunkid:64 copyversion:32 length:32
#define CSTOMA_CHUNKOP 153
// chunkid:64 version:32 newversion:32 copychunkid:64 copyversion:32 length:32 status:8

#define MATOCS_TRUNCATE 160
// chunkid:64 length:32 version:32 oldversion:32 
#define CSTOMA_TRUNCATE 161
// chunkid:64 status:8

#define MATOCS_DUPTRUNC 170
// chunkid:64 version:32 oldchunkid:64 oldversion:32 length:32
#define CSTOMA_DUPTRUNC 171
// chunkid:64 status:8

//CHUNKSERVER <-> CLIENT/CHUNKSERVER

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
#define CUTOCS_WRITE_FINISH 213
// chunkid:64 version:32

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

//CLIENT <-> MASTER

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
//   mode: FFFFMMMMMMMMMMMM
//         \--/\----------/
//           \       \------- mode
//            \-------------- flags
//
//   in case of BLOCKDEV and CHARDEV instead of 'length:64' on the end there is 'mojor:16 minor:16 empty:32'

// NAME type:
// ( leng:8 data:lengB )



#define FUSE_REGISTER_BLOB_NOACL       "kFh9mdZsR84l5e675v8bi54VfXaXSYozaU3DSz9AsLLtOtKipzb9aQNkxeOISx64"
// CUTOMA:
//  clientid:32 [ version:32 ]
// MATOCU:
//  clientid:32
//  status:8

#define FUSE_REGISTER_BLOB_TOOLS_NOACL "kFh9mdZsR84l5e675v8bi54VfXaXSYozaU3DSz9AsLLtOtKipzb9aQNkxeOISx63"
// CUTOMA:
//  -
// MATOCU:
//  status:8

#define FUSE_REGISTER_BLOB_ACL         "DjI1GAQDULI5d2YjA26ypc3ovkhjvhciTQVx3CS4nYgtBoUcsljiVpsErJENHaw0"

#define REGISTER_GETRANDOM 1
// rcode==1: generate random blob
// CUTOMA:
//  rcode:8
// MATOCU:
//  randomblob:32B

#define REGISTER_NEWSESSION 2
// rcode==2: first register
// CUTOMA:
//  rcode:8 version:32 ileng:32 info:ilengB pleng:32 path:plengB [ passcode:16B ]
// MATOCU:
//  sessionid:32 sesflags:8 rootuid:32 rootgid:32
//  status:8

#define REGISTER_RECONNECT 3
// rcode==3: mount reconnect
// CUTOMA:
//  rcode:8 sessionid:32 version:32
// MATOCU:
//  status:8

#define REGISTER_TOOLS 4
// rcode==4: tools connect
// CUTOMA:
//  rcode:8 sessionid:32 version:32
// MATOCU:
//  status:8

#define REGISTER_NEWMETASESSION 5
// rcode==5: first register
// CUTOMA:
//  rcode:8 version:32 ileng:32 info:ilengB [ passcode:16B ]
// MATOCU:
//  sessionid:32 sesflags:8
//  status:8

#define CUTOMA_FUSE_REGISTER 400
// blob:64B ... (depends on blob - see blob descriptions above)
#define MATOCU_FUSE_REGISTER 401
// depends on blob - see blob descriptions above
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
// msgid:32 inode:32 uid:32 gid:32
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
// msgid:32 inode:32 uid:32 gid:32 - old version (works like new version with flags==0)
// msgid:32 inode:32 uid:32 gid:32 flags:8
#define MATOCU_FUSE_GETDIR 429
// msgid:32 status:8
// msgid:32 N*[ name:NAME inode:32 type:8 ]	- when GETDIR_FLAG_WITHATTR in flags is not set
// msgid:32 N*[ name:NAME inode:32 type:35B ]	- when GETDIR_FLAG_WITHATTR in flags is set

#define CUTOMA_FUSE_OPEN 430
// msgid:32 inode:32 uid:32 gid:32 flags:8
#define MATOCU_FUSE_OPEN 431
// msgid:32 status:8

#define CUTOMA_FUSE_READ_CHUNK 432
// msgid:32 inode:32 chunkindx:32
#define MATOCU_FUSE_READ_CHUNK 433
// msgid:32 status:8
// msgid:32 length:64 chunkid:64 version:32 N*[ip:32 port:16]
// msgid:32 length:64 srcs:8 srcs*[chunkid:64 version:32 ip:32 port:16] - not implemented
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
// msgid:32 inode:32 srcinode:32 uid:32 gid:32 - append to existing element
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
// msgid:32 status:8
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
// msgid:32 inode:32 [opened:8] uid:32 gid:32 opened:8 length:64
#define MATOCU_FUSE_TRUNCATE 465
// msgid:32 status:8
// msgid:32 attr:35B

#define CUTOMA_FUSE_REPAIR 466
// msgid:32 inode:32 uid:32 gid:32
#define MATOCU_FUSE_REPAIR 467
// msgid:32 status:8
// msgid:32 notchanged:32 erased:32 repaired:32

#define CUTOMA_FUSE_SNAPSHOT 468
// msgid:32 inode:32 inode_dst:32 name_dst:NAME uid:32 gid:32 canoverwrite:8
#define MATOCU_FUSE_SNAPSHOT 469
// msgid:32 status:8

#define CUTOMA_FUSE_GETRESERVED 470
// msgid:32
#define MATOCU_FUSE_GETRESERVED 471
// msgid:32 status:8
// msgid:32 N*[ name:NAME inode:32 ]

#define CUTOMA_FUSE_GETEATTR 472
// msgid:32 inode:32 gmode:8
#define MATOCU_FUSE_GETEATTR 473
// msgid:32 status:8
// msgid:32 eattrdirs:8 eattrfiles:8 eattrdirs*[ eattr:8 dirs:32 ] eattrfiles*[ eattr:8 files:32 ]

#define CUTOMA_FUSE_SETEATTR 474
// msgid:32 inode:32 uid:32 eattr:8 smode:8
#define MATOCU_FUSE_SETEATTR 475
// msgid:32 status:8
// msgid:32 changed:32 notchanged:32 notpermitted:32

#define CUTOMA_FUSE_QUOTACONTROL 476
// msgid:32 inode:32 qflags:8 - delete quota
// msgid:32 inode:32 qflags:8 sinodes:32 slength:64 ssize:64 srealsize:64 hinodes:32 hlength:64 hsize:64 hrealsize:64 - set quota
#define MATOCU_FUSE_QUOTACONTROL 477
// msgid:32 status:8
// msgid:32 qflags:8 sinodes:32 slength:64 ssize:64 srealsize:64 hinodes:32 hlength:64 hsize:64 hrealsize:64 curinodes:32 curlength:64 cursize:64 currealsize:64


// special - reserved (opened) inodes - keep opened files.
#define CUTOMA_FUSE_RESERVED_INODES 499
// N*[inode:32]


// CLIENT <-> MASTER (stats - unregistered)

#define CUTOMA_CSERV_LIST 500
// -
#define MATOCU_CSERV_LIST 501
// 	N*[ip:32 port:16 used:64 total:64 chunks:32 tdused:64 tdtotal:64 tdchunks:32 errorcount:32 ]
// since version 1.5.13:
// 	N*[version:32 ip:32 port:16 used:64 total:64 chunks:32 tdused:64 tdtotal:64 tdchunks:32 errorcount:32 ]

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

#define CUTOMA_SESSION_LIST 508
// -
#define MATOCU_SESSION_LIST 509
// N*[ip:32 version:32 ]

#define CUTOMA_INFO 510
// -
#define MATOCU_INFO 511
// 	totalspace:64 availspace:64 trashspace:64 trashnodes:32 reservedspace:64 reservednodes:32 allnodes:32 dirnodes:32 filenodes:32 chunks:32 tdchunks:32
// since version 1.5.13:
// 	version:32 totalspace:64 availspace:64 trashspace:64 trashnodes:32 reservedspace:64 reservednodes:32 allnodes:32 dirnodes:32 filenodes:32 chunks:32 chunkcopies:32 tdcopies:32

#define CUTOMA_FSTEST_INFO 512
// -
#define MATOCU_FSTEST_INFO 513
// 	loopstart:32 loopend:32 files:32 ugfiles:32 mfiles:32 chunks:32 ugchunks:32 mchunks:32 msgleng:32 msgleng*[ char:8]
// since version 1.5.13
// 	loopstart:32 loopend:32 files:32 ugfiles:32 mfiles:32 msgleng:32 msgleng*[ char:8]

#define CUTOMA_CHUNKSTEST_INFO 514
// -
#define MATOCU_CHUNKSTEST_INFO 515
// loopstart:32 loopend:32 del_invalid:32 nodel_invalid:32 del_unused:32 nodel_unused:32 del_diskclean:32 nodel_diskclean:32 del_overgoal:32 nodel_overgoal:32 copy_undergoal:32 nocopy_undergoal:32 copy_rebalance:32

#define CUTOMA_CHUNKS_MATRIX 516
// -
#define MATOCU_CHUNKS_MATRIX 517
// 11*[11* count:32] - 11x11 matrix of chunks counters (goal x validcopies), 10 means 10 or more

#define CUTOMA_QUOTA_INFO 518
// -
#define MATOCU_QUOTA_INFO 519
// quota_time_limit:32 N * [ inode:32 pleng:32 path:plengB exceeded:8 qflags:8 stimestamp:32 sinodes:32 slength:64 ssize:64 sgoalsize:64 hinodes:32 hlength:64 hsize:64 hgoalsize:64 currinodes:32 currlength:64 currsize:64 currgoalsize:64 ]

#define CUTOMA_EXPORTS_INFO 520
// -
#define MATOCU_EXPORTS_INFO 521
// N * [ fromip:32 toip:32 pleng:32 path:plengB extraflags:8 sesflags:8 rootuid:32 rootgid:32 ]

#endif
