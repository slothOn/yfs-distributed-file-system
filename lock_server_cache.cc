// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "rpc.h"

static void *
revokethread(void *x)
{
  lock_server_cache *sc = (lock_server_cache *) x;
  sc->revoker();
  return 0;
}

static void *
retrythread(void *x)
{
  lock_server_cache *sc = (lock_server_cache *) x;
  sc->retryer();
  return 0;
}

lock_server_cache::lock_server_cache()
{
  pthread_t th;
  int r = pthread_create(&th, NULL, &revokethread, (void *) this);
  assert (r == 0);
  r = pthread_create(&th, NULL, &retrythread, (void *) this);
  assert (r == 0);

  this->lock_map = new std::unordered_map<lock_protocol::lockid_t, LockState>();
  pthread_mutex_init(&map_opr_mutex, NULL);
  pthread_mutex_init(&revoke_list_lock, NULL);
  pthread_mutex_init(&retry_list_lock, NULL);
}

lock_server_cache::~lock_server_cache()
{
  this->lock_map = new std::unordered_map<lock_protocol::lockid_t, LockState>();
  delete this->lock_map;
}

void
lock_server_cache::revoker()
{

  // This method should be a continuous loop, that sends revoke
  // messages to lock holders whenever another client wants the
  // same lock
  pthread_cond_init(&revoke_list_cond, NULL);
  while (true) {
    pthread_mutex_lock(&revoke_list_lock);
    while (revoke_list.empty()) {
      pthread_cond_wait(&revoke_list_cond, &revoke_list_lock);
    }
    ClientMsg clt_msg = revoke_list.front();
    revoke_list.pop_front();
    pthread_mutex_unlock(&revoke_list_lock);
    rpcc *cl = new rpcc(clt_msg.clt_entity->clt_d);
    int r;
    cl->call(rlock_protocol::revoke, cl->id(), clt_msg.lid, clt_msg.clt_entity->rpc_seq, r);
  }

}


void
lock_server_cache::retryer()
{

  // This method should be a continuous loop, waiting for locks
  // to be released and then sending retry messages to those who
  // are waiting for it.
  pthread_cond_init(&retry_list_cond, NULL);
  while (true) {
    pthread_mutex_lock(&retry_list_lock);
    while (retry_list.empty()) {
      pthread_cond_wait(&retry_list_cond, &retry_list_lock);
    }
    ClientMsg clt_msg = retry_list.front();
    retry_list.pop_front();
    pthread_mutex_unlock(&retry_list_lock);
    rpcc *cl = new rpcc(clt_msg.clt_entity->clt_d);
    int r;
    cl->call(rlock_protocol::retry, cl->id(), clt_msg.lid, clt_msg.clt_entity->rpc_seq, r);
  }
  
}

lock_protocol::status 
lock_server_cache::acquire(int clt, lock_protocol::lockid_t lid, int rpc_seq, std::string dst, int &)
{
  if (lock_map->count(lid) == 0) {
    pthread_mutex_lock(&map_opr_mutex);
    if (lock_map->count(lid) == 0) {
      LockState nlock_state;
      lock_map->insert(std::pair<lock_protocol::lockid_t, LockState>(lid, nlock_state));
    }
    pthread_mutex_unlock(&map_opr_mutex);  
  }
  LockState lock_state = lock_map->find(lid)->second;
  pthread_mutex_lock(&(lock_state.lock_mutex));
  ClientEntity nce(clt, dst, rpc_seq);
  int status;
  if (lock_state.state == lock_state_s::FREE) {
    lock_state.state = lock_state_s::LOCKED;
    lock_state.lock_owner = &nce;
    status = lock_protocol::OK;
  } else if (lock_state.state == lock_state_s::LOCKED) {
    lock_state.waitlist.push_back(&nce);

    ClientMsg revoke_msg;
    revoke_msg.clt_entity = lock_state.lock_owner;
    revoke_msg.lid = lid;
    pthread_mutex_lock(&revoke_list_lock);
    revoke_list.push_back(revoke_msg);
    pthread_mutex_unlock(&revoke_list_lock);
    pthread_cond_signal(&revoke_list_cond);

    status = lock_protocol::RETRY;
  } else if (lock_state.state == lock_state_s::REVOKE_SENT) {
    lock_state.waitlist.push_back(&nce);
    status = lock_protocol::RETRY;
  }
  pthread_mutex_unlock(&(lock_state.lock_mutex));
  return status;
}

lock_protocol::status 
lock_server_cache::release(int clt, lock_protocol::lockid_t lid, int &)
{
  LockState lock_state = lock_map->find(lid)->second;
  pthread_mutex_lock(&(lock_state.lock_mutex));
  
  lock_state.state = lock_state_s::FREE;
  delete lock_state.lock_owner;
  lock_state.lock_owner = NULL;

  if (!lock_state.waitlist.empty()) {
    ClientMsg retry_msg;
    retry_msg.clt_entity = lock_state.waitlist.front();
    lock_state.waitlist.pop_front();
    retry_msg.lid = lid;
    pthread_mutex_lock(&retry_list_lock);
    retry_list.push_back(retry_msg);
    pthread_mutex_unlock(&retry_list_lock);
    pthread_cond_signal(&retry_list_cond);
  }

  pthread_mutex_unlock(&(lock_state.lock_mutex));
  return lock_protocol::OK;  
}



