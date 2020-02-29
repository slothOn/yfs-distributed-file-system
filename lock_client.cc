// RPC stubs for clients to talk to lock_server

#include "lock_client.h"
#include "rpc.h"
#include <arpa/inet.h>

#include <sstream>
#include <iostream>
#include <stdio.h>

lock_client::lock_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() < 0) {
    printf("lock_client: call bind\n");
  }
}

int
lock_client::stat(lock_protocol::lockid_t lid)
{
  int r;
  int ret = cl->call(lock_protocol::stat, cl->id(), lid, r);
  assert (ret == lock_protocol::OK);
  return r;
}

lock_protocol::status
lock_client::acquire(lock_protocol::lockid_t lid)
{
<<<<<<< HEAD
=======
  int r;
  return cl->call(lock_protocol::acquire, cl->id(), lid, r);
>>>>>>> 5f4b8bd1be257b135da73f3ba854cb5e97908192
}

lock_protocol::status
lock_client::release(lock_protocol::lockid_t lid)
{
<<<<<<< HEAD
=======
  int r;
  printf("client release\n");
  return cl->call(lock_protocol::release, cl->id(), lid, r);
>>>>>>> 5f4b8bd1be257b135da73f3ba854cb5e97908192
}

