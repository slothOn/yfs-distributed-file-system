// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctime>


yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  srand(time(NULL));
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return ! isfile(inum);
}

int yfs_client::createfile(inum pinum, std::string file_name, bool is_dir, int& ninum)
{
  int r = OK;

  inum rinum = rand();
  if (is_dir) {
    rinum = rinum | 0x80000000;
  } else {
    rinum = rinum & 0x7fffffff;
  }

  ninum = rinum;

  std::string dircontent;
  ec->get(pinum, dircontent);
  dircontent
    .append(file_name).append("\t").append(filename(rinum)).append("\n");

  ec->put(pinum, dircontent);

  ec->put(rinum, "");

  return r;
}

yfs_client::inum yfs_client::ilookup(yfs_client::inum di, std::string name)
{
  std::string dircontent;
  ec->get(di, dircontent);

  std::string::size_type line_end = dircontent.find("\n");
  std::string::size_type start = 0;
  while (line_end != dircontent.npos) {
    std::string line = dircontent.substr(start, line_end - start);
    std::string::size_type split_tab = line.find("\t");
    std::string file_name = line.substr(0, split_tab);
    inum file_inum = n2i(line.substr(split_tab + 1, line.size() - file_name.size() - 1));
    if (name == file_name) {
      return file_inum;
    }
    
    if (isdir(file_inum)) {
      inum ret = ilookup(file_inum, name);
      if (ret != 0) {
        return ret;
      }
    }

    start = line_end + 1;
    line_end = dircontent.find("\n", start);
  }
  return 0;
}

int yfs_client::readfile(inum finum, std::string &content)
{
  return ec->get(finum, content);
}

int yfs_client::readdir(inum dinum, std::vector<std::pair<std::string, yfs_client::inum> > &diritems)
{
  std::string dircontent;
  ec->get(dinum, dircontent);

  std::string::size_type line_end = dircontent.find("\n");
  std::string::size_type start = 0;
  while (line_end != dircontent.npos) {
    std::string line = dircontent.substr(start, line_end - start);
    std::string::size_type split_tab = line.find("\t");
    std::string file_name = line.substr(0, split_tab);
    inum file_inum = n2i(line.substr(split_tab + 1, line.size() - file_name.size() - 1));
    
    diritems.push_back(std::make_pair(file_name, file_inum));

    start = line_end + 1;
    line_end = dircontent.find("\n", start);
  }
  return 0;
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  int r = OK;


  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

 release:

  return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;


  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

 release:
  return r;
}


