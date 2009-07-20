#ifndef _BGJOBS_H_
#define _BGJOBS_H_

void* job_pool_new(uint8_t workers,uint32_t jobs,int *wakeupdesc);
int job_pool_can_add(void *jpool);
void job_pool_check_jobs(void *jpool);
void job_pool_change_callback(void *jpool,uint32_t jobid,void (*callback)(uint8_t status,void *extra),void *extra);
void job_pool_delete(void *jpool);


uint32_t job_inval(void *jpool,void (*callback)(uint8_t status,void *extra),void *extra);
uint32_t job_chunkop(void *jpool,void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint32_t newversion,uint64_t copychunkid,uint32_t copyversion,uint32_t length);

#define job_delete(_jp,_cb,_ex,_chunkid,_version) job_chunkop(_jp,_cb,_ex,_chunkid,_version,0,0,0,0)
#define job_create(_jp,_cb,_ex,_chunkid,_version) job_chunkop(_jp,_cb,_ex,_chunkid,_version,0,0,0,1)
#define job_test(_jp,_cb,_ex,_chunkid,_version) job_chunkop(_jp,_cb,_ex,_chunkid,_version,0,0,0,2)
#define job_version(_jp,_cb,_ex,_chunkid,_version,_newversion) (((_newversion)>0)?job_chunkop(_jp,_cb,_ex,_chunkid,_version,_newversion,0,0,0xFFFFFFFF):job_inval(_jp,_cb,_ex))
#define job_truncate(_jp,_cb,_ex,_chunkid,_version,_newversion,_length) (((_newversion)>0&&(_length)!=0xFFFFFFFF)?job_chunkop(_jp,_cb,_ex,_chunkid,_version,_newversion,0,0,_length):job_inval(_jp,_cb,_ex))
#define job_duplicate(_jp,_cb,_ex,_chunkid,_version,_newversion,_copychunkid,_copyversion) (((_newversion>0)&&(_copychunkid)>0)?job_chunkop(_jp,_cb,_ex,_chunkid,_version,_newversion,_copychunkid,_copyversion,0xFFFFFFFF):job_inval(_jp,_cb,_ex))
#define job_duptrunc(_jp,_cb,_ex,_chunkid,_version,_newversion,_copychunkid,_copyversion,_length) (((_newversion>0)&&(_copychunkid)>0&&(_length)!=0xFFFFFFFF)?job_chunkop(_jp,_cb,_ex,_chunkid,_version,_newversion,_copychunkid,_copyversion,_length):job_inval(_jp,_cb,_ex))

uint32_t job_open(void *jpool,void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid);
uint32_t job_close(void *jpool,void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid);
uint32_t job_read(void *jpool,void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint16_t blocknum,uint8_t *buffer,uint32_t offset,uint32_t size,uint8_t *crcbuff);
uint32_t job_write(void *jpool,void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint16_t blocknum,const uint8_t *buffer,uint32_t offset,uint32_t size,const uint8_t *crcbuff);

/* srcs: srccnt * (chunkid:64 version:32 ip:32 port:16) */
uint32_t job_replicate(void *jpool,void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint8_t srccnt,const uint8_t *srcs);
uint32_t job_replicate_simple(void *jpool,void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint32_t ip,uint16_t port);

#endif
