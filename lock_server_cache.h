#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"
#include <pthread.h>
#include <list>
#include <unordered_map>

enum xxstate {FREE, LOCKED, REVOKE_SENT};
typedef enum xxstate lock_state_s;

class ClientEntity {
 public:
  int clt_id;
  sockaddr_in clt_d;
  int rpc_seq;

  ClientEntity(int clt_id, std::string dst, int rpc_seq)
  {
    this->clt_id = clt_id;
    make_sockaddr(dst.c_str(), &(this->clt_d));
    this->rpc_seq = rpc_seq;
  }
};

typedef struct {
  ClientEntity* clt_entity;
  lock_protocol::lockid_t lid;
} ClientMsg;

class LockState {
 public:
  pthread_mutex_t lock_mutex;
  std::list<ClientEntity*> waitlist;
  ClientEntity* lock_owner;

  lock_state_s state;

  LockState() 
  {
    pthread_mutex_init(&lock_mutex, NULL);
    lock_owner = NULL;
    state = FREE;
  }

  /*
  ~LockState() 
  {
    if (lock_owner != NULL) {
      delete lock_owner;
      lock_owner = NULL;
    }
    std::list<ClientEntity*>::iterator it;
    for (it = waitlist.begin(); it != waitlist.end(); it++) {
      if (*it != NULL) {
        delete *it;
        *it = NULL;
      }
    }
  }
  */
};

class lock_server_cache : public lock_server {
 protected:
  std::unordered_map<lock_protocol::lockid_t, LockState*>* lock_map;
  std::list<ClientMsg*> revoke_list, retry_list;
  pthread_mutex_t map_opr_mutex;

  pthread_mutex_t revoke_list_lock;
  pthread_cond_t revoke_list_cond;

  pthread_mutex_t retry_list_lock;
  pthread_cond_t retry_list_cond;

 public:
  lock_server_cache();
  ~lock_server_cache();
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int rpc_seq, std::string dst, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
  void revoker();
  void retryer();
};

#endif
