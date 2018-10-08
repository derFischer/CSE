// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2 part2 code goes here
  pthread_mutex_lock(&mutex);

  if(locks.find(lid) == locks.end())
  {
    pthread_cond_t cv = PTHREAD_COND_INITIALIZER;
    lock tmp;
    tmp.acquired = true;
    tmp.cv = cv;
    locks[lid] = tmp;
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  printf("acquire the lock %d\n", lid);
  while(locks[lid].acquired)
  {
    pthread_cond_wait(&locks[lid].cv, &mutex);
  }

  locks[lid].acquired = true;
  pthread_mutex_unlock(&mutex);
  return ret;
} 

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2 part2 code goes here
  pthread_mutex_lock(&mutex);

  printf("release the lock %d\n", lid);
  if(locks.find(lid) == locks.end() || !locks[lid].acquired)
  {
    ret = lock_protocol::NOENT;
    return ret;
  }

  locks[lid].acquired =false;
  pthread_mutex_unlock(&mutex);
  pthread_cond_signal(&locks[lid].cv);
  return ret;
}
