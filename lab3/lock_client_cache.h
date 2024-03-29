// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"


// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 6.

enum lockState { NONE, FREE, LOCKED, ACQUIRING, RELEASING };
struct lockInstance
{
  lockState state;
  int maxReqId;
  bool got;
  pthread_cond_t acquireCv;
  pthread_cond_t releaseCv;
  pthread_cond_t waitCv;
  pthread_cond_t freeCv;
};

class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user() {};
};

class lock_client_cache : public lock_client {
 private:
  class lock_release_user *lu;
  int rlock_port;
  std::string hostname;
  std::string id;
  std::map<lock_protocol::lockid_t, lockInstance> locks;
  pthread_mutex_t cm;
 public:
  static int last_port;
  lock_client_cache(std::string xdst, class lock_release_user *l = 0);
  virtual ~lock_client_cache() {};
  lock_protocol::status acquire(lock_protocol::lockid_t);
  lock_protocol::status release(lock_protocol::lockid_t);
  rlock_protocol::status revoke_handler(lock_protocol::lockid_t,
                                        int &);
  rlock_protocol::status retry_handler(lock_protocol::lockid_t, 
                                       int &);
  rlock_protocol::status grant_handler(lock_protocol::lockid_t,
                                       int &);
};


#endif
