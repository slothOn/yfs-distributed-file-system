// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>


static void *
releasethread(void *x)
{
  lock_client_cache *cc = (lock_client_cache *) x;
  cc->releaser();
  return 0;
}

int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // assert(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  /* register RPC handlers with rlsrpc */
  pthread_t th;
  int r = pthread_create(&th, NULL, &releasethread, (void *) this);
  assert (r == 0);

  this->lock_map = new std::unordered_map<lock_protocol::lockid_t, LockState*>();
  pthread_mutex_init(&map_opr_mutex, NULL);

  sockaddr_in xdstsock;
  make_sockaddr(xdst.c_str(), &xdstsock);
  cl = new rpcc(xdstsock);
  if (cl->bind() < 0) {
    printf("lock_client_cache: call bind\n");
  }
  this->xdst = xdst;
}


void
lock_client_cache::releaser()
{

  // This method should be a continuous loop, waiting to be notified of
  // freed locks that have been revoked by the server, so that it can
  // send a release RPC.
  
  pthread_mutex_init(&release_list_lock, NULL);
  pthread_cond_init(&release_list_cond, NULL);

  while (true) {
    pthread_mutex_lock(&release_list_lock);
    while () {
      
    }
    int r = 0;
    cl->call(lock_protocol::release, cl->id(), lid, rpc_seq, r);
  }

}
/**
 * FREE: call rpc to get lock
 *   if unavailable, set as ACQUIRING and blocked
 *   else LOCKED and return ok
 * LOCKED: return ok
 * ACQUIRING
 */ 
lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  if (lock_map->count(lid) == 0) {
    pthread_mutex_lock(&map_opr_mutex);
    LockState nlock_state;
    nlock_state.state = lock_state_c::UNKNOWN;
    lock_map->insert(std::pair<lock_protocol::lockid_t, LockState>(lid, nlock_state));
    pthread_mutex_unlock(&map_opr_mutex);
  }


  if (lock_map->count(lid) == 0) {
    pthread_mutex_lock(&map_opr_mutex);
    int r = 0;
    lock_protocal::status stat = cl->call(lock_protocol::acquire, cl->id(), lid, rpc_req, xdst, r);
    LockState nlock_state;
    if (stat == lock_protocol::OK) {
      nlock_state.state = lock_state_c::LOCKED;
      lock_map->insert(std::pair<lock_protocol::lockid_t, LockState>(lid, nlock_state));
      pthread_mutex_unlock(&map_opr_mutex);
      return lock_protocol::OK;
    } else if (stat == lock_protocol::RETRY) {
      nlock_state.state = lock_state_c::ACQUIRING;
      lock_map->insert(std::pair<lock_protocol::lockid_t, LockState>(lid, nlock_state));
      pthread_mutex_unlock(&map_opr_mutex);

      pthread_mutex_lock(&(nlock_state.lock_mutex));
      while (nlock_state.state != lock_protocol::FREE) {
        pthread_cond_wait(&(nlock_state.lock_cond), &(nlock_state.lock_mutex));
      }
      nlock_state.state = lock_state_c::LOCKED;
      pthread_mutex_unlock(&(nlock_state.lock_mutex));
      return lock_protocol::OK;
    } else {
      pthread_mutex_unlock(&map_opr_mutex);
      return lock_protocol::RPCERR;
    }   
  } 

  LockState lock_state = lock_map->find(lid)->second;
  if (lock_state.state == lock_state_c::LOCKED) {
    return lock_protocol::OK;
  } else if (lock_state.state == lock_state_c::FREE) {
    // call rpc to get lock from server
  } else if (lock_state.state == lock_state_c::ACQUIRING) {
    pthread_cond_wait();
  } else {

  }
  
}

// release in local.
lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  return lock_protocol::RPCERR;
}

lock_protocal::status
lock_client_cache::revoke() 
{}

