#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>


#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"
#include <queue>

struct req
{
  int reqId;
  pthread_cond_t clientReq;
};

struct lockInfo
{
  bool acquired;
  std::string owner;
  std::string giveTo;
  pthread_cond_t grantSuccess;
  std::queue<std::string> waitingQ;
};

class lock_server_cache {
 private:
  int nacquire;
  pthread_mutex_t sm;
  std::map<lock_protocol::lockid_t, lockInfo> owners;
  std::map<std::string, std::map<lock_protocol::lockid_t, req> > reqs;

 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int, int &);
  int release(lock_protocol::lockid_t, std::string id, int, int &);
  int grant(lock_protocol::lockid_t, std::string id, int &);
};

#endif
