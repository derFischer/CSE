#include "namenode.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sys/stat.h>
#include <unistd.h>
#include "threader.h"

using namespace std;

void NameNode::init(const string &extent_dst, const string &lock_dst)
{
  ec = new extent_client(extent_dst);
  lc = new lock_client_cache(lock_dst);
  yfs = new yfs_client(extent_dst, lock_dst);

  /* Add your init logic here */
}

list<NameNode::LocatedBlock> NameNode::GetBlockLocations(yfs_client::inum ino)
{
  printf("enter name node locatedblock\n");
  fflush(stdout);
  list<blockid_t> block_ids;
  if (ec->get_block_ids(ino, block_ids) != extent_protocol::OK)
  {
    printf("name node locatedblock finish failed\n");
    fflush(stdout);
    throw HdfsException("get block ids failed");
    return list<NameNode::LocatedBlock>();
  }
  int fileSize = block_ids.back();
  block_ids.pop_back();
  int offset = 0;
  int size = block_ids.size();

  list<NameNode::LocatedBlock> result;
  for (int index = 0; index < size && fileSize > 0; ++index)
  {
    result.push_back(LocatedBlock(block_ids.front(), offset, (fileSize % BLOCK_SIZE) ? fileSize % BLOCK_SIZE : BLOCK_SIZE, master_datanode));
    block_ids.pop_front();
    offset += BLOCK_SIZE;
    fileSize -= BLOCK_SIZE;
  }
  printf("name node locatedblock finish\n");
  fflush(stdout);
  return result;
}

bool NameNode::Complete(yfs_client::inum ino, uint32_t new_size)
{
  printf("enter name node complete\n");
  fflush(stdout);
  if (ec->complete(ino, new_size) != extent_protocol::OK)
  {
    lc->release(ino);
    printf("name node complete finish\n");
    fflush(stdout);
    throw HdfsException("complete failed");
    return false;
  }
  lc->release(ino);
  printf("name node complete finish\n");
  fflush(stdout);
  return true;
}

NameNode::LocatedBlock NameNode::AppendBlock(yfs_client::inum ino)
{
  printf("enter name node appendblock\n");
  fflush(stdout);
  blockid_t bid;
  extent_protocol::attr a;
  if (ec->getattr(ino, a) != extent_protocol::OK)
  {
    printf("name node appendblock finish\n");
    fflush(stdout);
    throw HdfsException("get attr failed");
    return LocatedBlock(0, 0, 0, master_datanode);
  }
  if (ec->append_block(ino, bid) != extent_protocol::OK)
  {
    printf("name node appendblock finish\n");
    fflush(stdout);
    throw HdfsException("append block failed");
    return LocatedBlock(0, 0, 0, master_datanode);
  }
  int offset = ((a.size / BLOCK_SIZE) + (a.size % BLOCK_SIZE != 0)) * BLOCK_SIZE;
  printf("name node appendblock finish\n");
  fflush(stdout);
  return LocatedBlock(bid, offset, BLOCK_SIZE, master_datanode);
}

bool NameNode::Rename(yfs_client::inum src_dir_ino, string src_name, yfs_client::inum dst_dir_ino, string dst_name)
{
  printf("enter name node rename\n");
  fflush(stdout);
  lc->acquire(src_dir_ino);
  bool found = false;
  yfs_client::inum inodeNum;
  if (yfs->Lookup(src_dir_ino, src_name.c_str(), found, inodeNum) != yfs_client::OK)
  {
    lc->release(src_dir_ino);
    printf("name node rename finish\n");
    fflush(stdout);
    throw HdfsException("lookup file failed");
    return false;
  }
  if (!found)
  {
    lc->release(src_dir_ino);
    printf("name node rename finish\n");
    fflush(stdout);
    throw HdfsException("rename a file that doesn't exist");
    return false;
  }
  yfs_client::dirent entry;
  entry.name = src_name;
  if (yfs->updateDirListRemove(src_dir_ino, entry) != yfs_client::OK)
  {
    lc->release(src_dir_ino);
    printf("name node rename finish\n");
    fflush(stdout);
    throw HdfsException("remove the name from src failed");
    return false;
  }
  lc->release(src_dir_ino);
  lc->acquire(dst_dir_ino);
  entry.inum = inodeNum;
  entry.name = dst_name;
  if (yfs->updateDirListAdd(dst_dir_ino, entry) != yfs_client::OK)
  {
    lc->release(dst_dir_ino);
    printf("name node rename finish\n");
    fflush(stdout);
    throw HdfsException("add the name to dst failed");
    return false;
  }
  lc->release(dst_dir_ino);
  printf("name node rename finish\n");
  fflush(stdout);
  return true;
}

bool NameNode::Mkdir(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out)
{
  printf("enter name node mkdir\n");
  fflush(stdout);
  if (yfs->mkdir(parent, name.c_str(), mode, ino_out) != yfs_client::OK)
  {
    printf("name node mkdir finish\n");
    fflush(stdout);
    throw HdfsException("mkdir failed");
    return false;
  }
  printf("name node mkdir finish\n");
  fflush(stdout);
  return true;
}

bool NameNode::Create(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out)
{
  printf("enter name node create\n");
  fflush(stdout);
  if (yfs->create(parent, name.c_str(), mode, ino_out) != yfs_client::OK)
  {
    printf("name node create finish\n");
    fflush(stdout);
    throw HdfsException("create a file failed");
    return false;
  }
  lc->acquire(ino_out);
  printf("name node create finish\n");
  fflush(stdout);
  return true;
}

bool NameNode::Isfile(yfs_client::inum ino)
{
  printf("enter name node isfile\n");
  fflush(stdout);
  return yfs->Isfile(ino);
}

bool NameNode::Isdir(yfs_client::inum ino)
{
  printf("enter name node isdir\n");
  fflush(stdout);
  return yfs->Isdir(ino);
}

bool NameNode::Getfile(yfs_client::inum ino, yfs_client::fileinfo &info)
{
  printf("enter name node get file\n");
  fflush(stdout);
  if (yfs->Getfile(ino, info) != yfs_client::OK)
  {
    printf("name node get file finish\n");
    fflush(stdout);
    throw HdfsException("get file info failed");
    return false;
  }
  return true;
}

bool NameNode::Getdir(yfs_client::inum ino, yfs_client::dirinfo &info)
{
  printf("enter name node get dir\n");
  fflush(stdout);
  if (yfs->Getdir(ino, info) != yfs_client::OK)
  {
    printf("name node get dir finish\n");
    fflush(stdout);
    throw HdfsException("get dir info failed");
    return false;
  }
  return true;
}

bool NameNode::Readdir(yfs_client::inum ino, std::list<yfs_client::dirent> &dir)
{
  printf("enter name node read dir\n");
  fflush(stdout);
  if (yfs->Readdir(ino, dir) != yfs_client::OK)
  {
    printf("name node read dir finish\n");
    fflush(stdout);
    throw HdfsException("read dir failed");
    return false;
  }
  return true;
}

bool NameNode::Unlink(yfs_client::inum parent, string name, yfs_client::inum ino)
{
  printf("enter name node unlink\n");
  fflush(stdout);
  if (yfs->Unlink(parent, name.c_str()) != yfs_client::OK)
  {
    printf("name node unlink finish\n");
    fflush(stdout);
    throw HdfsException("unlink failed");
    return false;
  }
  return true;
}

void NameNode::DatanodeHeartbeat(DatanodeIDProto id)
{
}

void NameNode::RegisterDatanode(DatanodeIDProto id)
{
}

list<DatanodeIDProto> NameNode::GetDatanodes()
{
  return list<DatanodeIDProto>();
}
