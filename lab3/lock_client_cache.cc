// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"

int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst,
                                     class lock_release_user *_lu)
    : lock_client(xdst), lu(_lu)
{
  pthread_mutex_init(&cm, NULL);
  srand(time(NULL) ^ last_port);
  rlock_port = ((rand() % 32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
  rlsrpc->reg(rlock_protocol::grant, this, &lock_client_cache::grant_handler);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  pthread_mutex_lock(&cm);
  if (locks.find(lid) == locks.end())
  {
    lockInstance tmp;
    tmp.state = NONE;
    tmp.maxReqId = 0;
    tmp.got = false;
    tmp.acquireCv = PTHREAD_COND_INITIALIZER;
    tmp.releaseCv = PTHREAD_COND_INITIALIZER;
    tmp.waitCv = PTHREAD_COND_INITIALIZER;
    tmp.freeCv = PTHREAD_COND_INITIALIZER;
    locks[lid] = tmp;
  }
  int thisReqId = locks[lid].maxReqId;
  tprintf("client %s try to acquire a lock %d with request ID %d\n", id, lid, thisReqId);
  while (true)
  {
    switch (locks[lid].state)
    {
    case NONE:
    {
      locks[lid].state = ACQUIRING;
      locks[lid].got = false;
      int reqId = locks[lid].maxReqId++;
      pthread_mutex_unlock(&cm);
      int r;
      ret = cl->call(lock_protocol::acquire, lid, id, reqId, r);
      pthread_mutex_lock(&cm);
      if (ret == lock_protocol::OK)
      {
        locks[lid].state = LOCKED;
        pthread_mutex_unlock(&cm);
        tprintf("client %s acquired a lock %d with request ID %d directly\n", id.c_str(), lid, thisReqId);
        return ret;
      }
      else if (ret == lock_protocol::WAIT)
      {
        printf("client %s acquired a lock %d with request ID %d failed and need to WAIT\n", id.c_str(), lid, thisReqId);
        if (!locks[lid].got)
        {
          pthread_cond_wait(&locks[lid].acquireCv, &cm);
        }
        printf("client %s WAITSUCCESS a lock %d with request ID %d\n", id.c_str(), lid, thisReqId);
        locks[lid].state = LOCKED;
        pthread_mutex_unlock(&cm);
        return ret;
      }
    }
    case FREE:
    {
      printf("client %s acquired a lock %d with request ID %d which is FREE and can be acquired directly\n", id.c_str(), lid, thisReqId);
      locks[lid].state = LOCKED;
      pthread_mutex_unlock(&cm);
      return ret;
    }
    case LOCKED:
    case ACQUIRING:
    {
      printf("client %s acquired a lock %d with request ID %d which is LOCKED/ACQUIRING and need to WAIT\n", id.c_str(), lid, thisReqId);
      pthread_cond_wait(&locks[lid].waitCv, &cm);
      break;
      /*if (!locks[lid].retry)
    {
      pthread_cond_wait(&locks[lid].waitCv, &cm);
      break;
    }
    else
    {
      locks[lid].retry = false;
      pthread_mutex_unlock(&cm);
      ret = cl->call(lock_protocol::acquire, lid, id, reqId, r);
      pthread_mutex_lock(&cm);
      if (ret == lock_protocol::OK)
      {
        locks[lid] = LOCKED;
        pthread_mutex_unlock(&cm);
        return ret;
      }
      else if (ret == lock_protocol::RETRY)
      {
        if (!locks[lid].retry)
        {
          pthread_cond_wait(&locks[lid].acquireCv, &cm);
        }
      }
      break;
    }*/
    }
    case RELEASING:
    {
      printf("client %s acquired a lock %d with request ID %d which is RELEASING and need to WAIT\n", id.c_str(), lid, thisReqId);
      pthread_cond_wait(&locks[lid].releaseCv, &cm);
      /*locks[lid].state = ACQUIRING;
    locks[lid].retry = false;
    int reqId = locks[lid].maxReqId++;
    pthread_mutex_unlock(&cm);
    ret = cl->call(lock_protocol::acquire, lid, id, reqId, r);
    pthread_mutex_lock(&cm);
    if (ret == lock_protocol::OK)
    {
      locks[lid] = LOCKED;
      pthread_mutex_unlock(&cm);
      return ret;
    }
    else if (ret == lock_protocol::RETRY)
    {
      while(!locks[lid].retry)
      {
        pthread_cond_wait(&locks[lid].acquireCv, &cm);
      }
      locks[lid].state = LOCKED;
      return ret;
    }*/
    }
    }
  }
  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  pthread_mutex_lock(&cm);
  if (locks[lid].state != LOCKED)
  {
  }
  else
  {
    locks[lid].state = FREE;
    pthread_cond_signal(&locks[lid].freeCv);
    pthread_cond_signal(&locks[lid].waitCv);
  }
  pthread_mutex_unlock(&cm);
  return ret;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid,
                                  int &)
{
  int ret = rlock_protocol::OK;
  pthread_mutex_lock(&cm);
  while (locks[lid].state != FREE)
  {
    pthread_cond_wait(&locks[lid].freeCv, &cm);
  }
  locks[lid].state = RELEASING;
  int reqId = locks[lid].maxReqId++;
  pthread_mutex_unlock(&cm);
  int r;
  ret = cl->call(lock_protocol::release, lid, id, reqId, r);
  pthread_mutex_lock(&cm);
  if (ret == lock_protocol::OK)
  {
    locks[lid].got = false;
    pthread_cond_signal(&locks[lid].releaseCv);
  }
  pthread_mutex_unlock(&cm);
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid,
                                 int &)
{
  /*pthread_mutex_lock(&cm);
  lid[lid].retry = true;
  pthread_cond_signal(&locks[lid].acquireCv);
  pthread_mutex_unlock(&cm);
  int ret = rlock_protocol::OK;
  return ret;*/
  return rlock_protocol::OK;
}

rlock_protocol::status
lock_client_cache::grant_handler(lock_protocol::lockid_t lid,
                                 int &)
{
  int ret = rlock_protocol::OK;
  pthread_mutex_lock(&cm);
  locks[lid].got = true;
  pthread_cond_signal(&locks[lid].acquireCv);
  pthread_mutex_unlock(&cm);
  return ret;
}