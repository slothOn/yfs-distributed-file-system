// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctime>
#include <unistd.h>


yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  lc = new lock_client(lock_dst);
  // srand(getpid());
  srand(time(NULL));
}

yfs_client::~yfs_client()
{
  delete ec;
  delete lc;
  ec = NULL;
  lc = NULL;
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
  
  pinum = pinum & num_mask;

  inum rinum = rand() % 0xffffffff + 1;
  if (!is_dir) {
    rinum = rinum | 0x80000000;
  } else {
    rinum = rinum & 0x7fffffff;
  }

  std::string dircontent;

  lock_protocol::lockid_t _lock_id = pinum;
  lc->acquire(_lock_id);
  ec->get(pinum, dircontent);
  
  std::string::size_type line_end = dircontent.find("\n");
  std::string::size_type start = 0;
  while (line_end != dircontent.npos) {
    std::string line = dircontent.substr(start, line_end - start);
    std::string::size_type split_tab = line.find("\t");
    std::string fname = line.substr(0, split_tab);
    
    if (file_name == fname) {
      inum fnum = n2i(line.substr(split_tab + 1, line.size() - fname.size() - 1));
      lc->release(_lock_id);
      ninum = fnum;
      return OK;
    }

    start = line_end + 1;
    line_end = dircontent.find("\n", start);
  }

  dircontent
    .append(file_name).append("\t").append(filename(rinum)).append("\n");
  ec->put(pinum, dircontent);
  lc->release(_lock_id);

  ec->put(rinum, "");
  ninum = rinum;

  return r;
}

yfs_client::inum yfs_client::ilookup(yfs_client::inum di, std::string name)
{
  di = di & num_mask;

  std::string dircontent;

  lock_protocol::lockid_t _lock_id = di;
  lc->acquire(_lock_id);
  ec->get(di, dircontent);
  lc->release(_lock_id);

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

int yfs_client::readdir(inum dinum, std::vector<std::pair<std::string, yfs_client::inum> > &diritems)
{
  dinum = dinum & num_mask;

  std::string dircontent;
  lock_protocol::lockid_t _lock_id = dinum;
  lc->acquire(_lock_id);
  ec->get(dinum, dircontent);
  lc->release(_lock_id);

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
  inum = inum & num_mask;
  lock_protocol::lockid_t _lock_id = inum;

  int r = OK;

  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  lc->acquire(_lock_id);
  int extent_status = ec->getattr(inum, a); 
  lc->release(_lock_id);
  if (extent_status != extent_protocol::OK) {
    printf("extent status %d\n", extent_status);
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
  inum = inum & num_mask;
  lock_protocol::lockid_t _lock_id = inum;

  int r = OK;

  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  lc->acquire(_lock_id);
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    lc->release(_lock_id);
    r = IOERR;
    goto release;
  }
  lc->release(_lock_id);
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

 release:
  return r;
}

int
yfs_client::setattr(inum fnum, fileinfo &fin)
{
  fnum = fnum & num_mask;
  printf("yfs client setattr file: %016llx, %d\n", fnum, fin.size);

  std::string content;
  lock_protocol::lockid_t _lock_id = fnum;
  ec->get(fnum, content);
  content.resize(fin.size);
  ec->put(fnum, content);
  lc->release(_lock_id);

  return OK;
}

int
yfs_client::readfile(inum fnum, size_t size, off_t off, char* buf)
{
  fnum = fnum & num_mask;
  printf("yfs client read file: %016llx, %d, %d\n", fnum, size, off);
  std::string content;
  lock_protocol::lockid_t _lock_id = fnum;
  lc->acquire(_lock_id);
  ec->get(fnum, content);
  lc->release(_lock_id);
  strncpy(buf, content.substr(off, size).data(), size);
  return OK;
}

int
yfs_client::writefile(inum fnum, size_t size, off_t off, const char* buf)
{
  fnum = fnum & num_mask;
  printf("yfs client write file: %d, %016llx, %d, %d\n", strlen(buf), fnum, size, off);
  std::string content;

  lock_protocol::lockid_t _lock_id = fnum;
  lc->acquire(_lock_id);
  ec->get(fnum, content);
  if (off >= content.size()) {
    content.resize(off, '\0');
    content.append(buf, size);  
  } else {
    content.replace(off, size, buf, size);
  }
  ec->put(fnum, content);
  lc->release(_lock_id);

  return OK;
}

int 
yfs_client::removefile(inum fnum, inum pinum)
{
  printf("yfs client");
  fnum = fnum & num_mask;
  pinum = pinum & num_mask;
  lock_protocol::lockid_t _lock_id = fnum;
  lock_protocol::lockid_t _lock_id2 = pinum;
  lc->acquire(_lock_id);
  lc->acquire(_lock_id2);
  ec->remove(fnum);

  std::string dircontent;
  ec->get(pinum, dircontent);
  std::string::size_type line_end = dircontent.find("\n");
  std::string::size_type start = 0;
  std::string::size_type found_start = std::string::npos;
  std::string::size_type found_end = std::string::npos;
  while (line_end != dircontent.npos) {
    std::string line = dircontent.substr(start, line_end - start);
    std::string::size_type split_tab = line.find("\t");
    std::string file_name = line.substr(0, split_tab);
    inum file_inum = n2i(line.substr(split_tab + 1, line.size() - file_name.size() - 1));
    
    if (file_inum == fnum) {
      found_start = start;  
      found_end = line_end;
      break;
    }

    start = line_end + 1;
    line_end = dircontent.find("\n", start);
  }
  if (found_start != std::string::npos) {
    dircontent = dircontent.substr(0, found_start) + dircontent.substr(found_end + 1);
  }
  ec->put(pinum, dircontent); 
  lc->release(_lock_id2);
  lc->release(_lock_id);
  return OK;
}


