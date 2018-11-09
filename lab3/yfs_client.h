#ifndef yfs_client_h
#define yfs_client_h

#include <string>

#include "lock_protocol.h"
#include "lock_client_cache.h"

//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>


class yfs_client {
  extent_client *ec;
  lock_client_cache *lc;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
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
  struct symlinkinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    yfs_client::inum inum;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);

 public:
  yfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);
  bool issymlink(inum inum);
  bool Isfile(inum);
  bool Isdir(inum);
  bool Issymlink(inum inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);
  int getsymlink(inum inum, symlinkinfo &symlink);
  int Getfile(inum, fileinfo &);
  int Getdir(inum, dirinfo &);
  int Getsymlink(inum inum, symlinkinfo &symlink);

  int setattr(inum, size_t);
  int Setattr(inum, size_t);
  int lookup(inum, const char *, bool &, inum &);
  int Lookup(inum, const char *, bool &, inum &);
  int create(inum, const char *, mode_t, inum &);
  int Create(inum, const char *, mode_t, inum &);
  int readdir(inum, std::list<dirent> &);
  int Readdir(inum, std::list<dirent> &);
  int write(inum, size_t, off_t, const char *, size_t &);
  int Write(inum, size_t, off_t, const char *, size_t &);
  int read(inum, size_t, off_t, std::string &);
  int Read(inum, size_t, off_t, std::string &);
  int unlink(inum,const char *);
  int Unlink(inum,const char *);
  int mkdir(inum , const char *, mode_t , inum &);
  int Mkdir(inum , const char *, mode_t , inum &);
  int readlink(inum ino, std::string &dest);
  int Readlink(inum ino, std::string &dest);
  int symlink(inum parent, const char *name, inum &ino_out, const char *dest);
  int Symlink(inum parent, const char *name, inum &ino_out, const char *dest);
  bool dupFileName(inum parent, const char *name);
  int updateDirListAdd(inum parent, dirent item);
  int updateDirListRemove(inum parent, dirent item);
  /** you may need to add symbolic link related methods here.*/
};

#endif 
