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

#include <time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <inttypes.h>
#include <netinet/in.h>

#include "MFSCommunication.h"

#include "datapack.h"
#include "matocsserv.h"
#include "cfg.h"
#include "main.h"
#include "sockets.h"
#include "chunks.h"
#include "random.h"

#define MaxPacketSize 50000000

// matocsserventry.mode
enum{KILL,HEADER,DATA};

typedef struct packetstruct {
	struct packetstruct *next;
	uint8_t *startptr;
	uint32_t bytesleft;
	uint8_t *packet;
} packetstruct;

typedef struct matocsserventry {
	uint8_t mode;
	int sock;
	time_t lastread,lastwrite;
	uint8_t hdrbuff[8];
	packetstruct inputpacket;
	packetstruct *outputhead,**outputtail;

	char *servstrip;		// human readable version of servip
	uint32_t servip;		// ip to coonnect to
	uint16_t servport;		// port to connect to
	uint16_t timeout;		// communication timeout
	uint64_t usedspace;		// used hdd space in bytes
	uint64_t totalspace;		// total hdd space in bytes
	uint32_t chunkscount;
	uint64_t todelusedspace;
	uint64_t todeltotalspace;
	uint32_t todelchunkscount;
	uint32_t errorcounter;

	uint32_t rndcarry;

	struct matocsserventry *next;
} matocsserventry;

static matocsserventry *matocsservhead=NULL;
static int lsock;

// from config
static char *ListenHost;
static char *ListenPort;

int matocsserv_compare(const void *a,const void *b) {
	const struct servsort {
		double space;
		void *ptr;
	} *aa=a,*bb=b;
	if (aa->space > bb->space) {
		return 1;
	}
	if (aa->space < bb->space) {
		return -1;
	}
	return 0;
}

void matocsserv_usagedifference(double *minusage,double *maxusage,uint16_t *usablescount,uint16_t *totalscount) {
	matocsserventry *eptr;
	uint32_t j,k;
	double minspace=1.0,maxspace=0.0;
	double space;
	j = 0;
	k = 0;
	for (eptr = matocsservhead ; eptr && j<65535 && k<65535; eptr=eptr->next) {
		if (eptr->mode!=KILL) {
			if (eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace) {
				space = (double)(eptr->usedspace) / (double)(eptr->totalspace);
				if (j==0) {
					minspace = maxspace = space;
				} else if (space<minspace) {
					minspace = space;
				} else if (space>maxspace) {
					maxspace = space;
				}
				j++;
			}
			k++;
		}
	}
	*usablescount = j;
	*totalscount = k;
	if (j==0) {
		*minusage = 1.0;
		*maxusage = 0.0;
	} else {
		*minusage = minspace;
		*maxusage = maxspace;
	}
}

uint16_t matocsserv_getservers_ordered(void* ptrs[65535],double maxusagediff,uint32_t *pmin,uint32_t *pmax) {
	static struct servsort {
		double space;
		void *ptr;
	} servsorttab[65535],servtab[65536];
	matocsserventry *eptr;
	uint32_t i,j,k,min,mid,max;
	double minspace=1.0,maxspace=0.0;
	double space;

//	syslog(LOG_NOTICE,"getservers start");
	j = 0;
	for (eptr = matocsservhead ; eptr && j<65535; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace) {
			space = (double)(eptr->usedspace) / (double)(eptr->totalspace);
			if (j==0) {
				minspace = maxspace = space;
			} else if (space<minspace) {
				minspace = space;
			} else if (space>maxspace) {
				maxspace = space;
			}
			servtab[j].ptr = eptr;
			servtab[j].space = space;
//			syslog(LOG_NOTICE,"ptr: %p, space:%lf",eptr,space);
			j++;
		}
	}
	if (j==0) {
//		syslog(LOG_NOTICE,"getservers - noservers");
		return 0;
	}

//	syslog(LOG_NOTICE,"getservers - minspace: %lf , maxspace: %lf , diff: %lf , maxusagediff: %lf",minspace,maxspace,maxspace-minspace,maxusagediff);
	if (maxspace-minspace<=maxusagediff*2) {
		maxusagediff = (maxspace-minspace)/2.0;
	}
	min = 0;
	max = j;
	mid = 0;
	for (i=0 ; i<j ; i++) {
		if (servtab[i].space-minspace<maxusagediff) {
			ptrs[min++]=servtab[i].ptr;
		} else if (maxspace-servtab[i].space<maxusagediff) {
			ptrs[--max]=servtab[i].ptr;
		} else {
			servsorttab[mid++]=servtab[i];
		}
	}

	// random <0-min)
	for (i=0 ; i<min ; i++) {
		// k = random <i,j)
		k = i+(rndu32()%(min-i));
		// swap(i,k)
		if (i!=k) {
			void* p = ptrs[i];
			ptrs[i] = ptrs[k];
			ptrs[k] = p;
		}
	}

	// random <max-j)
	for (i=max ; i<j ; i++) {
		// k = random <i,j)
		k = i+(rndu32()%(j-i));
		// swap(i,k)
		if (i!=k) {
			void* p = ptrs[i];
			ptrs[i] = ptrs[k];
			ptrs[k] = p;
		}
	}

	// sort <min-max)
	if (mid>0) {
		qsort(servsorttab,mid,sizeof(struct servsort),matocsserv_compare);
	}
	for (i=0 ; i<mid ; i++) {
		ptrs[min+i]=servsorttab[i].ptr;
	}
	if (pmin!=NULL) {
		*pmin=min;
	}
	if (pmax!=NULL) {
		*pmax=j-max;
	}
//		syslog(LOG_NOTICE,"getservers <0-%d) random ; <%d-%d) sort ; <%d-END) random",min,min,max,max);
//		for (i=0 ; i<j ; i++) {
//			syslog(LOG_NOTICE,"getservers - s%u : %p",i,ptrs[i]);
//		}
	return j;
}



uint16_t matocsserv_getservers_wrandom(void* ptrs[65535],uint16_t demand) {
	static struct servsort {
		uint32_t p;
		matocsserventry *ptr;
	} servtab[65536],x;
	matocsserventry *eptr;
	uint32_t psum;
	uint32_t r,j,i,k;
	j = 0;
	psum = 0;
	for (eptr = matocsservhead ; eptr && j<65535; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->totalspace>0 && eptr->usedspace<=eptr->totalspace && (eptr->totalspace - eptr->usedspace)>(1<<30)) {
			servtab[j].ptr=eptr;
			servtab[j].p = eptr->totalspace>>30;
			psum += servtab[j].p;
			j++;
		}
	}
	if (j==0) {
		return 0;
	}
	if (demand>j) {
		demand=j;
	}
	// pouk³adaæ servtab tak by na pocz±tku by³y serwery z rndcarry>0
	i = 0;
	k = j-1;
	while (i<k) {
		while (i<k && servtab[i].ptr->rndcarry>0) i++;
		while (i<k && servtab[k].ptr->rndcarry==0) k--;
		if (i<k) {
			x = servtab[i];
			servtab[i] = servtab[k];
			servtab[k] = x;
		}
	}
	for (k=0 ; k<demand ; k++) {
		if (servtab[k].ptr->rndcarry>0) {
			servtab[k].ptr->rndcarry--;
			// serwer wybrany bez losowania - przeniesienie z poprzednich losowañ
		} else {
			do {
				// r = random <0,psum)
				r = rndu32()%psum;
				// losuje jeden z 'j' serwerów z prawdopodobieñstwem servtab[i].p/psum (dla i od 0 do j-1) 
				for (i=0 ; i<j && r>=servtab[i].p ; i++) {
					r-=servtab[i].p;
				}
				if (i<k) {	// serwer ju¿ zosta³ wybrany wcze¶niej
					servtab[i].ptr->rndcarry++;
					r = 1;
				} else {
					r = 0;
					if (i>k) {
						x = servtab[i];
						servtab[i] = servtab[k];
						servtab[k] = x;
					}
				}
			} while (r);
		}
		ptrs[k] = servtab[k].ptr;
	}
	return demand;
}

void matocsserv_getspace(uint64_t *totalspace,uint64_t *availspace) {
	matocsserventry *eptr;
	uint64_t tspace,uspace;
	tspace = 0;
	uspace = 0;
	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->totalspace>0) {
			tspace += eptr->totalspace;
			uspace += eptr->usedspace;
		}
	}
	*totalspace = tspace;
	*availspace = tspace-uspace;
}

uint32_t matocsserv_cservlist_size(void) {
	matocsserventry *eptr;
	uint32_t i;
	i=0;
	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL) {
			i++;
		}
	}
	return i*(4+2+8+8+4+8+8+4+4);
}

void matocsserv_cservlist_data(uint8_t *ptr) {
	matocsserventry *eptr;
	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL) {
			PUT32BIT(eptr->servip,ptr);
			PUT16BIT(eptr->servport,ptr);
			PUT64BIT(eptr->usedspace,ptr);
			PUT64BIT(eptr->totalspace,ptr);
			PUT32BIT(eptr->chunkscount,ptr);
			PUT64BIT(eptr->todelusedspace,ptr);
			PUT64BIT(eptr->todeltotalspace,ptr);
			PUT32BIT(eptr->todelchunkscount,ptr);
			PUT32BIT(eptr->errorcounter,ptr);
		}
	}
}

void matocsserv_status(void) {
	matocsserventry *eptr;
	uint32_t n;
	uint64_t tspace,uspace;
	tspace = 0;
	uspace = 0;
	n=0;
	syslog(LOG_NOTICE,"chunkservers status:");
	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode!=KILL && eptr->totalspace>0) {
			tspace += eptr->totalspace;
			uspace += eptr->usedspace;
			n++;
			syslog(LOG_NOTICE,"server %d (%s): usedspace: %lld (%u GB), totalspace: %lld (%u GB), usage: %.2f%%",n,eptr->servstrip,eptr->usedspace,(uint32_t)(eptr->usedspace>>30),eptr->totalspace,(uint32_t)(eptr->totalspace>>30),(eptr->totalspace>0)?(100.0*eptr->usedspace)/eptr->totalspace:0.0);
		}
	}
	syslog(LOG_NOTICE,"total: usedspace: %lld (%u GB), totalspace: %lld (%u GB), usage: %.2f%%",uspace,(uint32_t)(uspace>>30),tspace,(uint32_t)(tspace>>30),(tspace>0)?(100.0*uspace)/tspace:0.0);
}

char* matocsserv_getstrip(void *e) {
	matocsserventry *eptr = (matocsserventry *)e;
	static char *empty="???";
	if (eptr->mode!=KILL && eptr->servstrip) {
		return eptr->servstrip;
	}
	return empty;
}

int matocsserv_getlocation(void *e,uint32_t *servip,uint16_t *servport) {
	matocsserventry *eptr = (matocsserventry *)e;
	if (eptr->mode!=KILL) {
		*servip = eptr->servip;
		*servport = eptr->servport;
		return 0;
	}
	return -1;
}

char* matocsserv_makestrip(uint32_t ip) {
	uint8_t *ptr,pt[4];
	uint32_t l,i;
	char *optr;
	ptr = pt;
	PUT32BIT(ip,ptr);
	l=0;
	for (i=0 ; i<4 ; i++) {
		if (pt[i]>=100) {
			l+=3;
		} else if (pt[i]>=10) {
			l+=2;
		} else {
			l+=1;
		}
	}
	l+=4;
	optr = malloc(l);
	snprintf(optr,l,"%u.%u.%u.%u",pt[0],pt[1],pt[2],pt[3]);
	optr[l-1]=0;
	return optr;
}

uint8_t* matocsserv_createpacket(matocsserventry *eptr,uint32_t type,uint32_t size) {
	packetstruct *outpacket;
	uint8_t *ptr;
	uint32_t psize;

	outpacket=(packetstruct*)malloc(sizeof(packetstruct));
	if (outpacket==NULL) {
		return NULL;
	}
	psize = size+8;
	outpacket->packet=malloc(psize);
	outpacket->bytesleft = psize;
	if (outpacket->packet==NULL) {
		free(outpacket);
		return NULL;
	}
	ptr = outpacket->packet;
	PUT32BIT(type,ptr);
	PUT32BIT(size,ptr);
	outpacket->startptr = (uint8_t*)(outpacket->packet);
	outpacket->next = NULL;
	*(eptr->outputtail) = outpacket;
	eptr->outputtail = &(outpacket->next);
	return ptr;
}

int matocsserv_send_createchunk(void *e,uint64_t chunkid,uint32_t version) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint8_t *data;

	if (eptr->mode!=KILL) {
		data = matocsserv_createpacket(eptr,MATOCS_CREATE,8+4);
		if (data==NULL) {
			return -1;
		}
		PUT64BIT(chunkid,data);
		PUT32BIT(version,data);
	}
	return 0;
}

void matocsserv_got_createchunk_status(matocsserventry *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t status;
	if (length!=8+1) {
		syslog(LOG_NOTICE,"CSTOMA_CREATE - wrong size (%d/9)",length);
		eptr->mode=KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET8BIT(status,data);
	chunk_got_create_status(eptr,chunkid,status);
	syslog(LOG_NOTICE,"(%s:%u) chunk: %lld creation status: %d",eptr->servstrip,eptr->servport,chunkid,status);
}

int matocsserv_send_deletechunk(void *e,uint64_t chunkid,uint32_t version) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint8_t *data;

	if (eptr->mode!=KILL) {
		data = matocsserv_createpacket(eptr,MATOCS_DELETE,8+4);
		if (data==NULL) {
			return -1;
		}
		PUT64BIT(chunkid,data);
		PUT32BIT(version,data);
	}
	return 0;
}

void matocsserv_got_deletechunk_status(matocsserventry *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t status;
	if (length!=8+1) {
		syslog(LOG_NOTICE,"CSTOMA_DELETE - wrong size (%d/9)",length);
		eptr->mode=KILL;
		return;
	}
	GET64BIT(chunkid,data)
	GET8BIT(status,data)
	chunk_got_delete_status(eptr,chunkid,status);
	syslog(LOG_NOTICE,"(%s:%u) chunk: %lld deletion status: %d",eptr->servstrip,eptr->servport,chunkid,status);
}

int matocsserv_send_replicatechunk(void *e,uint64_t chunkid,uint32_t version,void *from) {
	matocsserventry *eptr = (matocsserventry *)e;
	matocsserventry *fromeptr = (matocsserventry *)from;
	uint8_t *data;
	uint32_t fromip=fromeptr->servip;
	uint16_t fromport=fromeptr->servport;

	if (eptr->mode!=KILL) {
		data = matocsserv_createpacket(eptr,MATOCS_REPLICATE,8+4+4+2);
		if (data==NULL) {
			return -1;
		}
		PUT64BIT(chunkid,data);
		PUT32BIT(version,data);
		PUT32BIT(fromip,data);
		PUT16BIT(fromport,data);
	}
	return 0;
}

void matocsserv_got_replicatechunk_status(matocsserventry *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t version;
	uint8_t status;
	if (length!=8+4+1) {
		syslog(LOG_NOTICE,"CSTOMA_REPLICATE - wrong size (%d/13)",length);
		eptr->mode=KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET32BIT(version,data);
	GET8BIT(status,data);
	chunk_got_replicate_status(eptr,chunkid,version,status);
	syslog(LOG_NOTICE,"(%s:%u) chunk: %lld replication status: %d",eptr->servstrip,eptr->servport,chunkid,status);
}

int matocsserv_send_setchunkversion(void *e,uint64_t chunkid,uint32_t version,uint32_t oldversion) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint8_t *data;

	if (eptr->mode!=KILL) {
		data = matocsserv_createpacket(eptr,MATOCS_SET_VERSION,8+4+4);
		if (data==NULL) {
			return -1;
		}
		PUT64BIT(chunkid,data);
		PUT32BIT(version,data);
		PUT32BIT(oldversion,data);
	}
	return 0;
}

void matocsserv_got_setchunkversion_status(matocsserventry *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t status;
	if (length!=8+1) {
		syslog(LOG_NOTICE,"CSTOMA_SET_VERSION - wrong size (%d/9)",length);
		eptr->mode=KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET8BIT(status,data);
	chunk_got_setversion_status(eptr,chunkid,status);
	syslog(LOG_NOTICE,"(%s:%u) chunk: %lld set version status: %d",eptr->servstrip,eptr->servport,chunkid,status);
}


int matocsserv_send_duplicatechunk(void *e,uint64_t chunkid,uint32_t version,uint64_t oldchunkid,uint32_t oldversion) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint8_t *data;

	if (eptr->mode!=KILL) {
		data = matocsserv_createpacket(eptr,MATOCS_DUPLICATE,8+4+8+4);
		if (data==NULL) {
			return -1;
		}
		PUT64BIT(chunkid,data);
		PUT32BIT(version,data);
		PUT64BIT(oldchunkid,data);
		PUT32BIT(oldversion,data);
	}
	return 0;
}

void matocsserv_got_duplicatechunk_status(matocsserventry *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t status;
	if (length!=8+1) {
		syslog(LOG_NOTICE,"CSTOMA_DUPLICATE - wrong size (%d/9)",length);
		eptr->mode=KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET8BIT(status,data);
	chunk_got_duplicate_status(eptr,chunkid,status);
	syslog(LOG_NOTICE,"(%s:%u) chunk: %lld duplication status: %d",eptr->servstrip,eptr->servport,chunkid,status);
}

int matocsserv_send_truncatechunk(void *e,uint64_t chunkid,uint32_t length,uint32_t version,uint32_t oldversion) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint8_t *data;

	if (eptr->mode!=KILL) {
		data = matocsserv_createpacket(eptr,MATOCS_TRUNCATE,8+4+4+4);
		if (data==NULL) {
			return -1;
		}
		PUT64BIT(chunkid,data);
		PUT32BIT(length,data);
		PUT32BIT(version,data);
		PUT32BIT(oldversion,data);
	}
	return 0;
}

void matocsserv_got_truncatechunk_status(matocsserventry *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t status;
	if (length!=8+1) {
		syslog(LOG_NOTICE,"CSTOMA_TRUNCATE - wrong size (%d/9)",length);
		eptr->mode=KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET8BIT(status,data);
	chunk_got_truncate_status(eptr,chunkid,status);
//	matocsserv_notify(&(eptr->duplication),eptr,chunkid,status);
	syslog(LOG_NOTICE,"(%s:%u) chunk: %lld truncate status: %d",eptr->servstrip,eptr->servport,chunkid,status);
}

int matocsserv_send_duptruncchunk(void *e,uint64_t chunkid,uint32_t version,uint64_t oldchunkid,uint32_t oldversion,uint32_t length) {
	matocsserventry *eptr = (matocsserventry *)e;
	uint8_t *data;

	if (eptr->mode!=KILL) {
		data = matocsserv_createpacket(eptr,MATOCS_DUPTRUNC,8+4+8+4+4);
		if (data==NULL) {
			return -1;
		}
		PUT64BIT(chunkid,data);
		PUT32BIT(version,data);
		PUT64BIT(oldchunkid,data);
		PUT32BIT(oldversion,data);
		PUT32BIT(length,data);
	}
	return 0;
}

void matocsserv_got_duptruncchunk_status(matocsserventry *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint8_t status;
	if (length!=8+1) {
		syslog(LOG_NOTICE,"CSTOMA_DUPTRUNC - wrong size (%d/9)",length);
		eptr->mode=KILL;
		return;
	}
	GET64BIT(chunkid,data);
	GET8BIT(status,data);
	chunk_got_duptrunc_status(eptr,chunkid,status);
//	matocsserv_notify(&(eptr->duplication),eptr,chunkid,status);
	syslog(LOG_NOTICE,"(%s:%u) chunk: %lld duplication with truncate status: %d",eptr->servstrip,eptr->servport,chunkid,status);
}

void matocsserv_register(matocsserventry *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t chunkversion;
	matocsserventry *eaptr;
	uint32_t i,chunkcount;
	uint8_t rversion;

	if (eptr->servip>0 || eptr->servport>0) {
		syslog(LOG_WARNING,"got register message from registered chunk-server !!!");
		eptr->mode=KILL;
		return;
	}
	if ((length&1)==0) {
		if (length<22 || ((length-22)%12)!=0) {
			syslog(LOG_NOTICE,"CSTOMA_REGISTER (old ver.) - wrong size (%d/22+N*12)",length);
			eptr->mode=KILL;
			return;
		}
		GET32BIT(eptr->servip,data);
		GET16BIT(eptr->servport,data);
		GET64BIT(eptr->usedspace,data);
		GET64BIT(eptr->totalspace,data);
		length-=22;
	} else {
		GET8BIT(rversion,data);
		if (rversion!=1 && rversion!=2 && rversion!=3) {
			syslog(LOG_NOTICE,"CSTOMA_REGISTER - wrong version (%d/1..3)",rversion);
			eptr->mode=KILL;
			return;
		}
		if (rversion==1) {
			if (length<39 || ((length-39)%12)!=0) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 1) - wrong size (%d/39+N*12)",length);
				eptr->mode=KILL;
				return;
			}
			GET32BIT(eptr->servip,data);
			GET16BIT(eptr->servport,data);
			GET64BIT(eptr->usedspace,data);
			GET64BIT(eptr->totalspace,data);
			GET64BIT(eptr->todelusedspace,data);
			GET64BIT(eptr->todeltotalspace,data);
			length-=39;
		} else if (rversion==2) {
			if (length<47 || ((length-47)%12)!=0) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 2) - wrong size (%d/47+N*12)",length);
				eptr->mode=KILL;
				return;
			}
			GET32BIT(eptr->servip,data);
			GET16BIT(eptr->servport,data);
			GET64BIT(eptr->usedspace,data);
			GET64BIT(eptr->totalspace,data);
			GET32BIT(eptr->chunkscount,data);
			GET64BIT(eptr->todelusedspace,data);
			GET64BIT(eptr->todeltotalspace,data);
			GET32BIT(eptr->todelchunkscount,data);
			length-=47;
		} else {
			if (length<49 || ((length-49)%12)!=0) {
				syslog(LOG_NOTICE,"CSTOMA_REGISTER (ver 3) - wrong size (%d/49+N*12)",length);
				eptr->mode=KILL;
				return;
			}
			GET32BIT(eptr->servip,data);
			GET16BIT(eptr->servport,data);
			GET16BIT(eptr->timeout,data);
			GET64BIT(eptr->usedspace,data);
			GET64BIT(eptr->totalspace,data);
			GET32BIT(eptr->chunkscount,data);
			GET64BIT(eptr->todelusedspace,data);
			GET64BIT(eptr->todeltotalspace,data);
			GET32BIT(eptr->todelchunkscount,data);
			length-=47;
		}
	}
	if (eptr->servip==0) {
		tcpgetpeer(eptr->sock,&(eptr->servip),NULL);
	}
	eptr->servstrip = matocsserv_makestrip(eptr->servip);
	syslog(LOG_NOTICE,"chunkserver register - ip: %s, port: %u, usedspace: %lld (%u GB), totalspace: %lld (%u GB)",eptr->servstrip,eptr->servport,eptr->usedspace,(uint32_t)(eptr->usedspace>>30),eptr->totalspace,(uint32_t)(eptr->totalspace>>30));
	for (eaptr=matocsservhead ; eaptr ; eaptr=eaptr->next) {
		if (eptr!=eaptr && eaptr->mode!=KILL && eaptr->servip==eptr->servip && eaptr->servport==eptr->servport) {
			syslog(LOG_WARNING,"chunk-server already connected !!!");
			eptr->mode = KILL;
			return;
		}
	}
//	eptr->creation = NULL;
//	eptr->setversion = NULL;
//	eptr->duplication = NULL;
	chunkcount = length/(8+4);
	for (i=0 ; i<chunkcount ; i++) {
		GET64BIT(chunkid,data);
		GET32BIT(chunkversion,data);
		chunk_server_has_chunk(eptr,chunkid,chunkversion);
	}
}

void matocsserv_space(matocsserventry *eptr,uint8_t *data,uint32_t length) {
	if (length!=16 && length!=32 && length!=40) {
		syslog(LOG_NOTICE,"CSTOMA_SPACE - wrong size (%d/16|32|40)",length);
		eptr->mode=KILL;
		return;
	}
	GET64BIT(eptr->usedspace,data);
	GET64BIT(eptr->totalspace,data);
	if (length==40) {
		GET32BIT(eptr->chunkscount,data);
	}
	if (length>=32) {
		GET64BIT(eptr->todelusedspace,data);
		GET64BIT(eptr->todeltotalspace,data);
		if (length==40) {
			GET32BIT(eptr->todelchunkscount,data);
		}
	}
}

void matocsserv_chunk_damaged(matocsserventry *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t i;

	if (length%8!=0) {
		syslog(LOG_NOTICE,"CSTOMA_CHUNK_DAMAGED - wrong size (%d/N*8)",length);
		eptr->mode=KILL;
		return;
	}
	for (i=0 ; i<length/8 ; i++) {
		GET64BIT(chunkid,data);
		syslog(LOG_NOTICE,"(%s:%u) chunk: %lld is damaged",eptr->servstrip,eptr->servport,chunkid);
		chunk_damaged(eptr,chunkid);
	}
}

void matocsserv_chunks_lost(matocsserventry *eptr,uint8_t *data,uint32_t length) {
	uint64_t chunkid;
	uint32_t i;

	if (length%8!=0) {
		syslog(LOG_NOTICE,"CSTOMA_CHUNK_LOST - wrong size (%d/N*8)",length);
		eptr->mode=KILL;
		return;
	}
	for (i=0 ; i<length/8 ; i++) {
		GET64BIT(chunkid,data);
		syslog(LOG_NOTICE,"(%s:%u) chunk lost: %lld",eptr->servstrip,eptr->servport,chunkid);
		chunk_lost(eptr,chunkid);
	}
}

void matocsserv_error_occurred(matocsserventry *eptr,uint8_t *data,uint32_t length) {
	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"CSTOMA_ERROR_OCCURRED - wrong size (%d/0)",length);
		eptr->mode=KILL;
		return;
	}
	eptr->errorcounter++;
}

void matocsserv_broadcast_logstring(uint64_t version,uint8_t *logstr,uint32_t logstrsize) {
	matocsserventry *eptr;
	uint8_t *data;

	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		data = matocsserv_createpacket(eptr,MATOCS_STRUCTURE_LOG,9+logstrsize);
		if (data!=NULL) {
			// PUT32BIT(version,data);
			PUT8BIT(0xFF,data);
			PUT64BIT(version,data);
			memcpy(data,logstr,logstrsize);
		}
	}
}

void matocsserv_broadcast_logrotate() {
	matocsserventry *eptr;

	for (eptr = matocsservhead ; eptr ; eptr=eptr->next) {
		matocsserv_createpacket(eptr,MATOCS_STRUCTURE_LOG_ROTATE,0);
	}
}

void matocsserv_gotpacket(matocsserventry *eptr,uint32_t type,uint8_t *data,uint32_t length) {
	switch (type) {
		case ANTOAN_NOP:
			break;
		case CSTOMA_REGISTER:
			matocsserv_register(eptr,data,length);
			break;
		case CSTOMA_SPACE:
			matocsserv_space(eptr,data,length);
			break;
		case CSTOMA_CHUNK_DAMAGED:
			matocsserv_chunk_damaged(eptr,data,length);
			break;
		case CSTOMA_CHUNK_LOST:
			matocsserv_chunks_lost(eptr,data,length);
			break;
		case CSTOMA_ERROR_OCCURRED:
			matocsserv_error_occurred(eptr,data,length);
			break;
		case CSTOMA_CREATE:
			matocsserv_got_createchunk_status(eptr,data,length);
			break;
		case CSTOMA_DELETE:
			matocsserv_got_deletechunk_status(eptr,data,length);
			break;
		case CSTOMA_REPLICATE:
			matocsserv_got_replicatechunk_status(eptr,data,length);
			break;
		case CSTOMA_DUPLICATE:
			matocsserv_got_duplicatechunk_status(eptr,data,length);
			break;
		case CSTOMA_SET_VERSION:
			matocsserv_got_setchunkversion_status(eptr,data,length);
			break;
		case CSTOMA_TRUNCATE:
			matocsserv_got_truncatechunk_status(eptr,data,length);
			break;
		case CSTOMA_DUPTRUNC:
			matocsserv_got_duptruncchunk_status(eptr,data,length);
			break;
		default:
			syslog(LOG_NOTICE,"matocs: got unknown message (type:%d)",type);
			eptr->mode=KILL;
	}
}

void matocsserv_term(void) {
	matocsserventry *eptr,*eaptr;
	packetstruct *pptr,*paptr;
	syslog(LOG_INFO,"matocs: closing %s:%s",ListenHost,ListenPort);
	tcpclose(lsock);

	eptr = matocsservhead;
	while (eptr) {
		if (eptr->inputpacket.packet) {
			free(eptr->inputpacket.packet);
		}
		pptr = eptr->outputhead;
		while (pptr) {
			if (pptr->packet) {
				free(pptr->packet);
			}
			paptr = pptr;
			pptr = pptr->next;
			free(paptr);
		}
		eaptr = eptr;
		eptr = eptr->next;
		free(eaptr);
	}
	matocsservhead=NULL;
}

void matocsserv_read(matocsserventry *eptr) {
	int32_t i;
	uint32_t type,size;
	uint8_t *ptr;
	i=read(eptr->sock,eptr->inputpacket.startptr,eptr->inputpacket.bytesleft);
	if (i==0) {
		syslog(LOG_INFO,"matocs: connection lost");
		eptr->mode = KILL;
		return;
	}
	if (i<0) {
		syslog(LOG_INFO,"matocs: read error: %m");
		eptr->mode = KILL;
		return;
	}
	eptr->inputpacket.startptr+=i;
	eptr->inputpacket.bytesleft-=i;

	if (eptr->inputpacket.bytesleft>0) {
		return;
	}

	if (eptr->mode==HEADER) {
		ptr = eptr->hdrbuff+4;
		GET32BIT(size,ptr);

		if (size>0) {
			if (size>MaxPacketSize) {
				syslog(LOG_WARNING,"matocs: packet too long (%u/%u)",size,MaxPacketSize);
				eptr->mode = KILL;
				return;
			}
			eptr->inputpacket.packet = malloc(size);
			if (eptr->inputpacket.packet==NULL) {
				syslog(LOG_WARNING,"matocs: out of memory");
				eptr->mode = KILL;
				return;
			}
			eptr->inputpacket.bytesleft = size;
			eptr->inputpacket.startptr = eptr->inputpacket.packet;
			eptr->mode = DATA;
			return;
		}
		eptr->mode = DATA;
	}

	if (eptr->mode==DATA) {
		ptr = eptr->hdrbuff;
		GET32BIT(type,ptr);
		GET32BIT(size,ptr);

		eptr->mode=HEADER;
		eptr->inputpacket.bytesleft = 8;
		eptr->inputpacket.startptr = eptr->hdrbuff;

		matocsserv_gotpacket(eptr,type,eptr->inputpacket.packet,size);

		if (eptr->inputpacket.packet) {
			free(eptr->inputpacket.packet);
		}
		eptr->inputpacket.packet=NULL;
		return;
	}
}

void matocsserv_write(matocsserventry *eptr) {
	packetstruct *pack; 
	int32_t i;
	pack = eptr->outputhead;
	if (pack==NULL) {
		return;
	}
	i=write(eptr->sock,pack->startptr,pack->bytesleft);
	if (i<0) {
		syslog(LOG_INFO,"matocs: write error: %m");
		eptr->mode = KILL;
		return;
	}
	pack->startptr+=i;
	pack->bytesleft-=i;
	if (pack->bytesleft>0) {
		return;
	}
	free(pack->packet);
	eptr->outputhead = pack->next;
	if (eptr->outputhead==NULL) {
		eptr->outputtail = &(eptr->outputhead);
	}
	free(pack);
}

int matocsserv_desc(fd_set *rset,fd_set *wset) {
	int max=lsock;
	matocsserventry *eptr;
	int i;
	FD_SET(lsock,rset);
	for (eptr=matocsservhead ; eptr ; eptr=eptr->next) {
		i=eptr->sock;
		FD_SET(i,rset);
		if (eptr->outputhead!=NULL) FD_SET(i,wset);
		if (i>max) max=i;
	}
	return max;
}

void matocsserv_serve(fd_set *rset,fd_set *wset) {
	uint32_t now=main_time();
	matocsserventry *eptr,**kptr;
	packetstruct *pptr,*paptr;
	int ns;

	if (FD_ISSET(lsock,rset)) {
		ns=tcpaccept(lsock);
		if (ns<0) {
			syslog(LOG_INFO,"matocs: accept error: %m");
		} else {
			tcpnonblock(ns);
			eptr = malloc(sizeof(matocsserventry));
			eptr->next = matocsservhead;
			matocsservhead = eptr;
			eptr->sock = ns;
			eptr->mode = HEADER;
			eptr->lastread = now;
			eptr->lastwrite = now;
			eptr->inputpacket.next = NULL;
			eptr->inputpacket.bytesleft = 8;
			eptr->inputpacket.startptr = eptr->hdrbuff;
			eptr->inputpacket.packet = NULL;
			eptr->outputhead = NULL;
			eptr->outputtail = &(eptr->outputhead);

			eptr->servstrip=NULL;
			eptr->servip=0;
			eptr->servport=0;
			eptr->timeout=60;
			eptr->usedspace=0;
			eptr->totalspace=0;
			eptr->chunkscount=0;
			eptr->todelusedspace=0;
			eptr->todeltotalspace=0;
			eptr->todelchunkscount=0;
			eptr->errorcounter=0;

			eptr->rndcarry=0;
//				eptr->creation=NULL;
//				eptr->deletion=NULL;
//				eptr->setversion=NULL;
//				eptr->duplication=NULL;
//				eptr->replication=NULL;
		}
	}
	for (eptr=matocsservhead ; eptr ; eptr=eptr->next) {
		if (FD_ISSET(eptr->sock,rset) && eptr->mode!=KILL) {
			eptr->lastread = now;
			matocsserv_read(eptr);
		}
		if (FD_ISSET(eptr->sock,wset) && eptr->mode!=KILL) {
			eptr->lastwrite = now;
			matocsserv_write(eptr);
		}
		if ((uint32_t)(eptr->lastread+eptr->timeout)<(uint32_t)now) {
			eptr->mode = KILL;
		}
		if ((uint32_t)(eptr->lastwrite+(eptr->timeout/2))<(uint32_t)now && eptr->outputhead==NULL) {
			matocsserv_createpacket(eptr,ANTOAN_NOP,0);
		}
	}
	kptr = &matocsservhead;
	while ((eptr=*kptr)) {
		if (eptr->mode == KILL) {
			chunk_server_disconnected(eptr);
			tcpclose(eptr->sock);
			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
			pptr = eptr->outputhead;
			while (pptr) {
				if (pptr->packet) {
					free(pptr->packet);
				}
				paptr = pptr;
				pptr = pptr->next;
				free(paptr);
			}
			if (eptr->servstrip) {
				free(eptr->servstrip);
			}
			*kptr = eptr->next;
			free(eptr);
		} else {
			kptr = &(eptr->next);
		}
	}
}

int matocsserv_init(void) {
	config_getnewstr("MATOCS_LISTEN_HOST","*",&ListenHost);
	config_getnewstr("MATOCS_LISTEN_PORT","9420",&ListenPort);

	lsock = tcpsocket();
	tcpnonblock(lsock);
	tcpreuseaddr(lsock);
	tcplisten(lsock,ListenHost,ListenPort,5);
	if (lsock<0) {
		syslog(LOG_ERR,"matocs: listen error: %m");
		return -1;
	}
	syslog(LOG_NOTICE,"matocs: listen on %s:%s",ListenHost,ListenPort);

	matocsservhead = NULL;
	main_destructregister(matocsserv_term);
	main_selectregister(matocsserv_desc,matocsserv_serve);
	main_timeregister(60,0,matocsserv_status);
	return 0;
}
