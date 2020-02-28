// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

#include<unordered_map>

lock_server::lock_server():
  nacquire (0)
{
  this->lock_map = new std::unordered_map<lock_protocol::lockid_t, Lock*>();
  pthread_mutex_init(&map_opr_mutex, NULL);
}

lock_server::~lock_server()
{
  std::unordered_map<lock_protocol::lockid_t, Lock*>::iterator iter;
  for (iter = lock_map->begin(); iter != lock_map->end(); iter++) {
    delete iter->second;
  }
  delete this->lock_map;
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
  // printf("acquire request from clt %d for lid %d\n", clt, lid);

  Lock* lock = NULL;
  if (lock_map->count(lid) == 0) {
    pthread_mutex_lock(&map_opr_mutex);
    lock = new Lock();
    lock_map->insert(std::pair<lock_protocol::lockid_t, Lock*>(lid, lock));
    pthread_mutex_unlock(&map_opr_mutex);
  } else {
    lock = lock_map->find(lid)->second;
  }
  
  pthread_mutex_t mutex = lock->GetMutex(); 
  pthread_mutex_lock(&mutex);
  while (lock->IfLocked()) {
    pthread_cond_wait(&(lock->GetCond()), &mutex);
  }
  lock->SetLocked(true);
  pthread_mutex_unlock(&mutex);

  return lock_protocol::OK;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  // printf("release request from clt %d for lid %d\n", clt, lid);

  Lock* lock = lock_map->find(lid)->second;

  /*
   There is no nead to lock the release assuming 
   we do not have malicious clients who call the release without getting the lock first.
  */
  // pthread_mutex_t mutex = lock->GetMutex(); 
  // pthread_mutex_lock(&mutex);
  lock->SetLocked(false);
  // pthread_mutex_unlock(&mutex);

  pthread_cond_signal(&(lock->GetCond()));
  return lock_protocol::OK;
}


