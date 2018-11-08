// lock protocol

#ifndef lock_protocol_h
#define lock_protocol_h

#include "rpc.h"

class lock_protocol {
 public:
  enum xxstatus { OK, WAIT, RPCERR, NOENT, IOERR, EXPIRED };
  typedef int status;
  typedef unsigned long long lockid_t;
  enum rpc_numbers {
    acquire = 0x7001,
    release,
    stat
  };
};

class rlock_protocol {
public:
    enum xxstatus { OK, RPCERR };
    typedef int status;
    enum rpc_numbers {
        revoke = 0x8001,
        retry = 0x8002,
        grant = 0x8003
    };
};

#endif 
