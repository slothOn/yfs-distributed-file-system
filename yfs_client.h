#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include "lock_client.h"
#include <vector>


class yfs_client {
  extent_client *ec;
  lock_client *lc;
  unsigned long long num_mask = 0xffffffff;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, FBIG };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    unsigned long long inum;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);
 public:

  yfs_client(std::string, std::string);
  ~yfs_client();

  bool isfile(inum);
  bool isdir(inum);
  inum ilookup(inum di, std::string name);
  int createfile(inum pinum, std::string file_name, bool is_dir, int& ninum);
  int readdir(inum inum, std::vector<std::pair<std::string, yfs_client::inum> > &diritems);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  int setattr(inum fnum, fileinfo &);
  int readfile(inum fnum, size_t size, off_t off, char* buf);
  int writefile(inum fnum, size_t size, off_t off, const char* buf);
  int removefile(inum fnum, inum pinum);
};

#endif 
