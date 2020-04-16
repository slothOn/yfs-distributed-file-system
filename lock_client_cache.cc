// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>

// helper method.
bool SendRpcPreCheck(LockState& lock_state);
bool SendRpcAfterCheck(LockState& lock_state);

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
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke);

  pthread_t th;
  int r = pthread_create(&th, NULL, &releasethread, (void *) this);
  assert (r == 0);

  this->lock_map = new std::unordered_map<lock_protocol::lockid_t, LockState>();
  pthread_mutex_init(&map_opr_mutex, NULL);

  sockaddr_in xdstsock;
  make_sockaddr(xdst.c_str(), &xdstsock);
  cl = new rpcc(xdstsock);
  if (cl->bind() < 0) {
    printf("lock_client_cache: call bind\n");
  }
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
    while (release_list.empty()) {
      pthread_cond_wait(&release_list_cond, &release_list_lock);
    }
    lock_protocol::lockid_t lid = release_list.front();
    release_list.pop_front();
    pthread_mutex_unlock(&release_list_lock);

    LockState& lock_state = lock_map->find(lid)->second;
    pthread_mutex_lock(&(lock_state.lock_mutex));
    while (lock_state.state != lock_state_c::FREE) {
      pthread_cond_wait(&(lock_state.lock_cond), &(lock_state.lock_mutex));
    }
    lock_state.state = lock_state_c::RELEASING;
    pthread_mutex_unlock(&(lock_state.lock_mutex));
    pthread_cond_signal(&(lock_state.lock_cond));

    int r = 0;
    cl->call(lock_protocol::release, cl->id(), lid, r);
    printf("release rpc sent for %llu\n", lid);
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
    lock_map->insert(std::pair<lock_protocol::lockid_t, LockState>(lid, nlock_state));
    pthread_mutex_unlock(&map_opr_mutex);
  }

  LockState& lock_state = lock_map->find(lid)->second;

  while (true) {
    pthread_mutex_lock(&(lock_state.lock_mutex));

    if (lock_state.state == lock_state_c::RELEASING) {
      
      /* send rpc starts. */
      if (!SendRpcPreCheck(lock_state)) {
        pthread_mutex_unlock(&(lock_state.lock_mutex));
        continue;
      }
      pthread_mutex_unlock(&(lock_state.lock_mutex));

      int r = 0;
      lock_protocol::status rpcstat = cl->call(lock_protocol::acquire, cl->id(), lid, lock_state.rpc_seq, id, r);
      
      pthread_mutex_lock(&(lock_state.lock_mutex));

      if (!SendRpcAfterCheck(lock_state)) {
        pthread_mutex_unlock(&(lock_state.lock_mutex));
        continue;
      }

      lock_state.rpc_state = rpc_state_c::ACQ_RECV;

      if (rpcstat == lock_protocol::OK) {
        lock_state.state = lock_state_c::LOCKED;
        pthread_mutex_unlock(&(lock_state.lock_mutex));
        return lock_protocol::OK;
      } else if (rpcstat == lock_protocol::RETRY) {
        lock_state.state = lock_state_c::ACQUIRING;
        while (lock_state.rpc_state != rpc_state_c::RETRY_RECV) {
          pthread_cond_wait(&(lock_state.lock_cond), &(lock_state.lock_mutex));
        }
        pthread_mutex_unlock(&(lock_state.lock_mutex));
        continue;
      } else {
        pthread_mutex_unlock(&(lock_state.lock_mutex));
        return lock_protocol::RPCERR;
      }
      /* send rpc ends. */

    } else if (lock_state.state == lock_state_c::LOCKED) {
      while (lock_state.state == lock_state_c::LOCKED) {
        pthread_cond_wait(&(lock_state.lock_cond), &(lock_state.lock_mutex));
      }
      pthread_mutex_unlock(&(lock_state.lock_mutex));
    } else if (lock_state.state == lock_state_c::ACQUIRING) {
      if (lock_state.rpc_state == rpc_state_c::RETRY_RECV) {
        
        /* send rpc starts. */
        if (!SendRpcPreCheck(lock_state)) {
          pthread_mutex_unlock(&(lock_state.lock_mutex));
          continue;
        }

        pthread_mutex_unlock(&(lock_state.lock_mutex));

        int r = 0;
        lock_protocol::status rpcstat = cl->call(lock_protocol::acquire, cl->id(), lid, lock_state.rpc_seq, id, r);
        
        pthread_mutex_lock(&(lock_state.lock_mutex));

        if (!SendRpcAfterCheck(lock_state)) {
          pthread_mutex_unlock(&(lock_state.lock_mutex));
          continue;
        }

        lock_state.rpc_state = rpc_state_c::ACQ_RECV;

        if (rpcstat == lock_protocol::OK) {
          lock_state.state = lock_state_c::LOCKED;
          pthread_mutex_unlock(&(lock_state.lock_mutex));
          return lock_protocol::OK;
        } else if (rpcstat == lock_protocol::RETRY) {
          lock_state.state = lock_state_c::ACQUIRING;
          while (lock_state.rpc_state != rpc_state_c::RETRY_RECV) {
            pthread_cond_wait(&(lock_state.lock_cond), &(lock_state.lock_mutex));
          }
          pthread_mutex_unlock(&(lock_state.lock_mutex));
          continue;
        } else {
          pthread_mutex_unlock(&(lock_state.lock_mutex));
          return lock_protocol::RPCERR;
        }
        /* send rpc ends. */

      } else {
        pthread_cond_wait(&(lock_state.lock_cond), &(lock_state.lock_mutex));
      }     
      pthread_mutex_unlock(&(lock_state.lock_mutex));
    } else {
      // FREE
      while (lock_state.state == lock_state_c::FREE
              && lock_state.rpc_state == rpc_state_c::REVOKE_RECV) {
        pthread_cond_wait(&(lock_state.lock_cond), &(lock_state.lock_mutex));
      }
      if (lock_state.state == lock_state_c::FREE) {
        lock_state.state = lock_state_c::LOCKED;
        pthread_mutex_unlock(&(lock_state.lock_mutex));
        return lock_protocol::OK;  
      }
      pthread_mutex_unlock(&(lock_state.lock_mutex));
    }

  }

  return lock_protocol::IOERR;
}

// release in local.
lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  if (lock_map->count(lid) == 0) {
    return lock_protocol::IOERR;
  }
  LockState& lock_state = lock_map->find(lid)->second;
  pthread_mutex_lock(&(lock_state.lock_mutex));
  if (lock_state.state != lock_state_c::LOCKED) {
    pthread_mutex_unlock(&(lock_state.lock_mutex));
    return lock_protocol::IOERR;  
  }
  lock_state.state = lock_state_c::FREE;
  pthread_mutex_unlock(&(lock_state.lock_mutex));
  pthread_cond_signal(&(lock_state.lock_cond));
  return lock_protocol::OK;
}

rlock_protocol::status 
lock_client_cache::revoke(lock_protocol::lockid_t lid, int rpc_seq, int &)
{
  printf("revoke request received for lid: %llu\n", lid);

  if (lock_map->count(lid) == 0) {
    return rlock_protocol::RPCERR;
  }
  
  LockState& lock_state = lock_map->find(lid)->second;
  pthread_mutex_lock(&(lock_state.lock_mutex));
  if (rpc_seq != lock_state.rpc_seq) {
    pthread_mutex_unlock(&(lock_state.lock_mutex));
    return rlock_protocol::RPCERR;
  }
  lock_state.rpc_state = rpc_state_c::REVOKE_RECV;
  pthread_mutex_unlock(&(lock_state.lock_mutex));

  pthread_mutex_lock(&release_list_lock);
  release_list.push_back(lid);
  pthread_mutex_unlock(&release_list_lock);
  pthread_cond_signal(&release_list_cond);

  return rlock_protocol::OK;
}

rlock_protocol::status 
lock_client_cache::retry(lock_protocol::lockid_t lid, int rpc_seq, int &)
{
  if (lock_map->count(lid) == 0) {
    return rlock_protocol::RPCERR;
  }

  LockState& lock_state = lock_map->find(lid)->second;
  pthread_mutex_lock(&(lock_state.lock_mutex));
  if (rpc_seq != lock_state.rpc_seq) {
    pthread_mutex_unlock(&(lock_state.lock_mutex));
    return rlock_protocol::RPCERR;
  }
  lock_state.rpc_state = rpc_state_c::RETRY_RECV;
  pthread_mutex_unlock(&(lock_state.lock_mutex));
  pthread_cond_signal(&(lock_state.lock_cond));
  return rlock_protocol::OK;
}

bool SendRpcPreCheck(LockState& lock_state)
{
  while (lock_state.rpc_state == rpc_state_c::ACQ_SENT) {
    pthread_cond_wait(&(lock_state.lock_cond), &(lock_state.lock_mutex));
  }
  if (lock_state.state != lock_state_c::RELEASING) {
    pthread_mutex_unlock(&(lock_state.lock_mutex));
    return false;
  }
  lock_state.rpc_seq++;
  lock_state.rpc_state = rpc_state_c::ACQ_SENT;
  lock_state.state = lock_state_c::ACQUIRING;
  return true;
}

bool SendRpcAfterCheck(LockState& lock_state)
{
  if (lock_state.rpc_state == rpc_state_c::REVOKE_RECV) {
    lock_state.state = lock_state_c::FREE;
    pthread_cond_signal(&(lock_state.lock_cond));
    while (lock_state.state == lock_state_c::FREE
          && lock_state.rpc_state == rpc_state_c::REVOKE_RECV) {
      pthread_cond_wait(&(lock_state.lock_cond), &(lock_state.lock_mutex));
    }
    return false;
  }

  if (lock_state.rpc_state == rpc_state_c::RETRY_RECV) {
    lock_state.state = lock_state_c::ACQUIRING;
    return false;
  }
  return true;
}
