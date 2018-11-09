// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

lock_server_cache::lock_server_cache()
{
  pthread_mutex_init(&sm, NULL);
}

int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, int reqId,
                               int &r)
{
  //printf("server: client %s try to acquire a lock %d with request ID %d\n", id.c_str(), lid, reqId);
  lock_protocol::status ret = lock_protocol::OK;
  r = lock_protocol::OK;
  pthread_mutex_lock(&sm);
  //if it is a stale req
  if (reqs[id][lid].reqId >= reqId)
  {
    //printf("server: client %s try to acquire a lock %d with request ID %d STALE\n", id.c_str(), lid, reqId);
    ret = r = lock_protocol::EXPIRED;
    pthread_mutex_unlock(&sm);
    return ret;
  }

  //wait for the previous req to be done
  while (reqs[id][lid].reqId < reqId - 1)
  {
    pthread_cond_wait(&reqs[id][lid].clientReq, &sm);
  }

  //the reqId should be updated first in case of duplicated request handle
  reqs[id][lid].reqId = reqId;
  //deal with the req
  if (owners.find(lid) == owners.end() || owners[lid].acquired == false)
  {
    //printf("server: client %s try to acquire a lock %d with request ID %d SUCCESS\n", id.c_str(), lid, reqId);
    lockInfo tmp;
    tmp.owner = id;
    tmp.giveTo = id;
    tmp.acquired = true;
    tmp.grantSuccess = PTHREAD_COND_INITIALIZER;
    owners[lid] = tmp;
  }
  else
  {
    std::string owner;
    if (owners[lid].waitingQ.empty())
    {
      owner = owners[lid].owner;
    }
    else
    {
      owner = owners[lid].waitingQ.back();
    }
    owners[lid].waitingQ.push(id);
    //printf("server: client %s try to acquire a lock %d with request ID %d WAIT previous owner: %s\n", id.c_str(), lid, reqId, owner.c_str());
    //wait for the grant process to be done
    while (owners[lid].owner != owners[lid].giveTo || owners[lid].owner != owner)
    {
      pthread_cond_wait(&owners[lid].grantSuccess, &sm);
    }

    //revoke the current owner
    handle h(owner);
    rpcc *cl = h.safebind();
    if (cl)
    {
      pthread_mutex_unlock(&sm);
      int r;
      ret = cl->call(rlock_protocol::revoke, lid, r);
      pthread_mutex_lock(&sm);
    }
    else
    {
      printf("bind failed\n");
    }
    //printf("server: revoke client %s lockid: %d\n", owner.c_str(), lid);
    //printf("server: client %s try to acquire a lock %d with request ID %d WAIT\n", id.c_str(), lid, reqId);
    ret = r = lock_protocol::WAIT;
  }
  pthread_cond_broadcast(&reqs[id][lid].clientReq);
  pthread_mutex_unlock(&sm);
  return ret;
}

int lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, int reqId,
                               int &r)
{
  //printf("server: client %s try to release a lock %d with request ID %d WAIT\n", id.c_str(), lid, reqId);
  lock_protocol::status ret = lock_protocol::OK;
  pthread_mutex_lock(&sm);
  //if it is a stale req
  if (reqs[id][lid].reqId >= reqId)
  {
    ret = r = lock_protocol::EXPIRED;
    pthread_mutex_unlock(&sm);
    return ret;
  }

  //wait for the previous req to be done
  while (reqs[id][lid].reqId < reqId - 1)
  {
    pthread_cond_wait(&reqs[id][lid].clientReq, &sm);
  }

  //deal with the req
  reqs[id][lid].reqId = reqId;

  //if the waiting queue is empty, mark the lock as free
  if (owners[lid].waitingQ.empty())
  {
    owners[lid].acquired = false;
  }
  else
  {
    //else, grant the lock to the next client
    std::string next = owners[lid].waitingQ.front();
    owners[lid].waitingQ.pop();
    owners[lid].giveTo = next;

    handle h(next);
    rpcc *cl = h.safebind();
    if (cl)
    {
      pthread_mutex_unlock(&sm);
      int r;
      ret = cl->call(rlock_protocol::grant, lid, r);
      pthread_mutex_lock(&sm);
    }
    else
    {
      printf("bind failed\n");
    }
    if (ret == rlock_protocol::OK)
    {
      pthread_cond_broadcast(&owners[lid].grantSuccess);
      owners[lid].owner = next;
      //printf("server: client %s try to release a lock %d with request ID %d SUCCESS and give to %s\n", id.c_str(), lid, reqId, next.c_str());
    }
  }
  pthread_cond_broadcast(&reqs[id][lid].clientReq);
  pthread_mutex_unlock(&sm);
  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}
