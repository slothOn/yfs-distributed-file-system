// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctime>

extent_server::extent_server() {}


int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  unsigned int cur_time = time_t(NULL);
  if (this->meta_map.count(id) == 0) {
    extent_protocol::attr file_attr;  
    file_attr.atime = cur_time;
    file_attr.ctime = cur_time;
    file_attr.mtime = cur_time;
    file_attr.size = buf.size();
    this->meta_map[id] = file_attr;
  } else {
    extent_protocol::attr& file_attr = this->meta_map[id];  
    file_attr.atime = cur_time;
    file_attr.mtime = cur_time;
    file_attr.size = buf.size();
  }
  this->map[id] = buf;
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  if (this->meta_map.count(id) == 0) {
    return extent_protocol::NOENT;
  }
  unsigned int cur_time = time_t(NULL);
  extent_protocol::attr& file_attr = this->meta_map[id];  
  file_attr.atime = cur_time;
  buf = this->map[id];
  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  if (this->meta_map.count(id) == 0) {
    return extent_protocol::NOENT;
  }
  a = this->meta_map[id];
  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  if (this->meta_map.count(id) == 0) {
    return extent_protocol::NOENT;
  }
  this->meta_map.erase(id);
  this->map.erase(id);
  return extent_protocol::OK;
}

