// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"

#include <pthread.h>
#include <unordered_map>

class Lock {
  private:
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int nacquire; // unused for now.
    bool locked;
  
  public:
    Lock(): 
    nacquire(0), locked(false)
    {
      pthread_mutex_init(&mutex, NULL);
      pthread_cond_init(&cond, NULL);
    }
    ~Lock() {}

    pthread_mutex_t& GetMutex()
    {
      return this->mutex;
    }

    pthread_cond_t& GetCond()
    {
      return this->cond;
    }

    bool IfLocked()
    {
      return this->locked;
    }

    void SetLocked(bool val)
    {
      this->locked = val;
    }
};

class lock_server {

 protected:
  int nacquire;
  std::unordered_map<lock_protocol::lockid_t, Lock*>* lock_map;
  pthread_mutex_t map_opr_mutex;

 public:
  lock_server();
  ~lock_server();
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  virtual lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int rpc_seq, std::string dst, int &) = 0;
  virtual lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif 

