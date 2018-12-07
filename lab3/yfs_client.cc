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

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  lc = new lock_client_cache(lock_dst);
  if (ec->put(1, "") != extent_protocol::OK)
      printf("error init root dir\n"); // XYB: init root dir
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
    printf("enter yfs is file\n");
    fflush(stdout);
    lc->acquire(inum);
    bool result = Isfile(inum);
    lc->release(inum);
    return result;
}

bool
yfs_client::Isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is a dir\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */
bool
yfs_client::isdir(inum inum)
{
    printf("enter yfs is dir\n");
    fflush(stdout);
    // Oops! is this still correct when you implement symlink?
    lc->acquire(inum);
    bool result = Isdir(inum);
    lc->release(inum);
    return result;
}

bool
yfs_client::Isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        printf("error getting attr\n");
        return false;
    }
    if (a.type == extent_protocol::T_DIR)
    {
        return true;
    }
    return false;
}

bool
yfs_client::issymlink(inum inum)
{
    printf("enter yfs is symlink\n");
    fflush(stdout);
    // Oops! is this still correct when you implement symlink?
    lc->acquire(inum);
    bool result = Issymlink(inum);
    lc->release(inum);
    return result;
}

bool
yfs_client::Issymlink(inum inum)
{
    extent_protocol::attr a;
    if(ec->getattr(inum, a) != extent_protocol::OK)
    {
        printf("error getting attr\n");
        return false;
    }
    if (a.type == extent_protocol::T_SYMLINK)
    {
        return true;
    }
    return false;
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    printf("enter yfs get file\n");
    fflush(stdout);
    lc->acquire(inum);
    int r = Getfile(inum, fin);
    lc->release(inum);
    return r;
}

int
yfs_client::Getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("IOERR GETFILE\n");
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
    printf("enter yfs get dir\n");
    fflush(stdout);
    lc->acquire(inum);
    int r = Getdir(inum, din);
    lc->release(inum);
    return r;
}

int
yfs_client::Getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("IOERR GETDIR\n");
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}

int
yfs_client::getsymlink(inum inum, symlinkinfo &symlink)
{
    printf("enter yfs get symlink\n");
    fflush(stdout);
    lc->acquire(inum);
    int r = Getsymlink(inum, symlink);
    lc->release(inum);
    return r;
}

int
yfs_client::Getsymlink(inum inum, symlinkinfo &symlink)
{
    int r = OK;

    printf("getsymlink %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("IOERR GETSYMLINK\n");
        r = IOERR;
        goto release;
    }
    symlink.size = a.size;
    symlink.atime = a.atime;
    symlink.mtime = a.mtime;
    symlink.ctime = a.ctime;

release:
    return r;
}

#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr

int
yfs_client::setattr(inum ino, size_t size)
{
    printf("enter yfs set attr\n");
    fflush(stdout);
    lc->acquire(ino);
    int r = Setattr(ino, size);
    lc->release(ino);
    return r;
}

int
yfs_client::Setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    std::string file;
    if(ec->get(ino, file) != extent_protocol::OK)
    {
        printf("IOERR: setattr: load the file failed.\n");
        return IOERR;
    }
    if(size != file.length())
    {
        file.resize(size);
        if(ec->put(ino, file) != extent_protocol::OK)
        {
            printf("IOERR:setattr: set the file failed.\n");
            return IOERR;
        }
    }
    return r;
}

bool
yfs_client::dupFileName(inum parent, const char *name)
{
    bool found = false;
    inum inodeNum;
    if(Lookup(parent, name, found, inodeNum) != OK)
    {
        printf("look up the dir list failed.\n");
        return true;
    }
    else
    {
        return found;
    }
}

int
yfs_client::updateDirListAdd(inum parent, dirent item)
{
    std::list<dirent> results;
    if(Readdir(parent, results) != OK)
    {
        printf("IOERR：update failed due to load dir content failed.\n");
        return IOERR;
    }
    results.push_back(item);
    
    std::stringstream dir;
    for(std::list<dirent>::iterator entry = results.begin(); entry != results.end(); ++entry)
    {
        dir << entry->name;
        dir << '\0';
        dir << entry->inum;
    }

    if(ec->put(parent, dir.str()) != extent_protocol::OK)
    {
        printf("IOERR：load the updated dir failed.\n");
        return IOERR;
    }
    return OK;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    printf("enter yfs create\n");
    fflush(stdout);
    printf("want to create a file under %d\n", parent);
    lc->acquire(parent);
    int r = Create(parent, name, mode, ino_out);
    lc->release(parent);
    printf("want to create a file under %d success!\n", parent);
    return r;
}

int
yfs_client::Create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    if(dupFileName(parent, name))
    {
        printf("dup file name found.\n");
        return EXIST;
    }
    else
    {
    printf("create the new file %s.\n", name);
        if(ec->create(extent_protocol::T_FILE, ino_out) != extent_protocol::OK)
        {
            printf("IOERR: add the new file failed.\n");
            return IOERR;
        }
        else
        {
            dirent entry;
            entry.name = name;
            entry.inum = ino_out;
            if(updateDirListAdd(parent, entry) != OK)
            {
                printf("IOERR: update dir file failed.\n");
                return IOERR;
            }
        }
    printf("create file %s with inode %d.\n", name, ino_out);
    }
    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    printf("enter yfs mkdir\n");
    fflush(stdout);
    lc->acquire(parent);
    int r = Mkdir(parent, name, mode, ino_out);
    lc->release(parent);
    return r;
}

int
yfs_client::Mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    if(dupFileName(parent, name))
    {
        printf("dup dir/file name found.\n");
        return EXIST;
    }
    else
    {
        if(ec->create(extent_protocol::T_DIR, ino_out) != extent_protocol::OK)
        {
            printf("IOERR: add the new dir file failed.\n");
            return IOERR;
        }
        else
        {
            dirent entry;
            entry.name = name;
            entry.inum = ino_out;
            if(updateDirListAdd(parent, entry) != OK)
            {
                printf("IOERR: update dir file failed.\n");
                return IOERR;
            }
        }
    }
    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    printf("enter yfs lookup, parent inum %d\n", parent);
    fflush(stdout);
    lc->acquire(parent);
    printf("yfs lookup acquire the lock of inum %d\n", parent);
    int r = Lookup(parent, name, found, ino_out);
    lc->release(parent);
    printf("yfs lookup release the lock of inum %d\n", parent);
    return r;
}

int
yfs_client::Lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    found = false;
    std::list<dirent> results;
    if(Readdir(parent, results) != OK)
    {
        printf("IOERR: fail to load the dir lists.\n");
        return IOERR;
    }

    else
    {
        for(std::list<dirent>::iterator entry = results.begin(); entry != results.end(); ++entry)
        {
            if(entry->name == name)
            {
                found = true;
                ino_out = entry->inum;
            }
        }
    }
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    printf("enter yfs readdir\n");
    fflush(stdout);
    lc->acquire(dir);
    int r = Readdir(dir, list);
    lc->release(dir);
    return r;
}

int
yfs_client::Readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    std::string d;
    if(ec->get(dir, d) != extent_protocol::OK)
    {
        printf("IOERR: fail to get the content of dir.\n");
        return IOERR;
    }
    else
    {
    list.clear();
    std::stringstream directory(d);
        dirent entry;
        while(getline(directory, entry.name, '\0'))
        {
            directory >> entry.inum;
            list.push_back(entry);
        }
    }
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    printf("enter yfs read\n");
    fflush(stdout);
    lc->acquire(ino);
    int r = Read(ino, size, off, data);
    lc->release(ino);
    return r;
}

int
yfs_client::Read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    if(ec->get(ino, data) != extent_protocol::OK)
    {
        printf("IOERR: read: load the file failed.\n");
        return IOERR;
    }
    data = data.substr(off);
    data.resize(size);
    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    printf("enter yfs write\n");
    fflush(stdout);
    lc->acquire(ino);
    int r = Write(ino, size, off, data, bytes_written);
    lc->release(ino);
    return r;
}

int
yfs_client::Write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    std::string originFile;
    std::string addContent = std::string(data, size);
    if (ec->get(ino, originFile) != extent_protocol::OK)
    {
        printf("IOERR: load the content of file failed.\n");
        return IOERR;
    }

    std::string originFile2 = originFile;
    originFile.resize(off, '\0');
    originFile += addContent;

    if(off + size < originFile2.size())
    {
        originFile += originFile2.substr(off + size);
    }

    if (ec->put(ino, originFile) != extent_protocol::OK)
    {
        printf("IOERR: write the file failed\n");
        return IOERR;
    }
    return r;
}

int
yfs_client::updateDirListRemove(inum parent, dirent item)
{
    std::list<dirent> results;
    if(Readdir(parent, results) != OK)
    {
        printf("IOERR: update failed due to load dir content failed.\n");
        return IOERR;
    }

    std::stringstream dir;
    for(std::list<dirent>::iterator entry = results.begin(); entry != results.end(); ++entry)
    {
    if(entry->name == item.name)
    {
        continue;
    }
        dir << entry->name;
        dir << '\0';
        dir << entry->inum;
    }

    if(ec->put(parent, dir.str()) != extent_protocol::OK)
    {
        printf("IOERR: load the updated dir failed.\n");
        return IOERR;
    }
    return OK;
}

int yfs_client::unlink(inum parent,const char *name)
{
    printf("enter yfs unlink\n");
    fflush(stdout);
    lc->acquire(parent);
    int r = Unlink(parent, name);
    lc->release(parent);
    return r;
}

int yfs_client::Unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    bool found = false;
    inum inodeNum;
    if(Lookup(parent, name, found, inodeNum) != OK)
    {
        printf("IOERR: unlink:look up the file failed.\n");
        return IOERR;
    }
    if(!found)
    {
        printf("IOERR: unlink:the file doesn't exist.\n");
        return IOERR;
    }
    dirent entry;
    entry.name = name;
    if(ec->remove(inodeNum) != extent_protocol::OK)
    {
        printf("IOERR: unlink:remove the file failed.\n");
        return IOERR;
    }
    if(updateDirListRemove(parent, entry) != OK)
    {
        printf("IOERR: unlink the entry failed.\n");
        return IOERR;
    }
    return r;
}

int
yfs_client::symlink(inum parent, const char *name, inum &ino_out, const char *dest)
{
    printf("enter yfs symlink\n");
    fflush(stdout);
    lc->acquire(parent);
    int r = Symlink(parent, name, ino_out, dest);
    lc->release(parent);
    return r;
}

int
yfs_client::Symlink(inum parent, const char *name, inum &ino_out, const char *dest)
{
    int r = OK;
    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    if(dupFileName(parent, name))
    {
        printf("dup file name found.\n");
        return EXIST;
    }
    else
    {
        if(ec->create(extent_protocol::T_SYMLINK, ino_out) != extent_protocol::OK)
        {
            printf("IOERR: add the new file failed.\n");
            return IOERR;
        }
        else
        {
            std::string path = dest;
            if(ec->put(ino_out, path) != extent_protocol::OK)
            {
                printf("IOERR: update the link content failed.\n");
                return IOERR;
            }
            dirent entry;
            entry.name = name;
            entry.inum = ino_out;
            if(updateDirListAdd(parent, entry) != OK)
            {
                printf("IOERR: update dir file failed.\n");
                return IOERR;
            }
        }
    }
    return r;
}

int
yfs_client::readlink(inum ino, std::string &dest)
{
    printf("enter yfs read link\n");
    fflush(stdout);
    lc->acquire(ino);
    int r = Readlink(ino, dest);
    lc->release(ino);
    return r;
}

int
yfs_client::Readlink(inum ino, std::string &dest)
{
    int r = OK;
    if(!Issymlink(ino))
    {
        printf("IOERR: try to read a file that is not a symlink.\n");
        return IOERR;
    }
    if(ec->get(ino, dest) != extent_protocol::OK)
    {
        printf("IOERR: fail to get the content of the symlink file.\n");
        return IOERR;
    }
    return r;
}