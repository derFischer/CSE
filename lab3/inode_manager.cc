#include "inode_manager.h"
#include <vector>
// disk layer -----------------------------------------
using namespace std;

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------
#define IF_BLOCK_ALLOC(a, i) ((a) & (1 << (i)))
#define ALLOC_BLOCK(a, i) ((a) | (1 << (i)))
#define FREE_BLOCK(a, i) ((a) & ~(1 << (i)))
// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  pthread_mutex_lock(&bitmapLock); 
  blockid_t block = Alloc_block();
  pthread_mutex_unlock(&bitmapLock); 
  return block;
}
blockid_t
block_manager::Alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  char tmp[BLOCK_SIZE];
  for(blockid_t b = 0; b < BLOCK_NUM; b += BPB)
  {
    read_block(BBLOCK(b), tmp);
    for(uint32_t i = 0; i < BLOCK_SIZE; ++i)
    {
      char bits = tmp[i];
      for(uint32_t offset = 0; offset < 8; ++offset)
      {
        if(!IF_BLOCK_ALLOC(bits, 7 - offset))
        {
          bits = ALLOC_BLOCK(bits, 7 - offset);
          tmp[i] = bits;
          write_block(BBLOCK(b), tmp);
          return b + 8 * i + offset;
        }
      }
    }
  }
  return 0;
}

void
block_manager::free_block(uint32_t id)
{
  pthread_mutex_lock(&bitmapLock); 
  Free_block(id);
  pthread_mutex_unlock(&bitmapLock); 
  return;
}
void
block_manager::Free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  char tmp[BLOCK_SIZE];
  read_block(BBLOCK(id), tmp);
  uint32_t offset = (id % BPB);
  uint32_t char_index = offset / 8;
  uint32_t char_offset = offset - 8 * char_index;
  char t = tmp[char_index];
  tmp[char_index] = FREE_BLOCK(t, 7 - char_offset);
  write_block(BBLOCK(id), tmp);

  char empty[BLOCK_SIZE];
  memset(empty, 0, BLOCK_SIZE);
  write_block(id, empty);
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();
  pthread_mutex_init(&bitmapLock,NULL);  
  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

    //alloc boot block
  alloc_block();

  //alloc super block
  alloc_block();

  //alloc bitmap block
  for(int i = 0; i < BLOCK_NUM; i += BPB)
  {
    alloc_block();
  }

  for(int i = 0; i < INODE_NUM; i += IPB)
  {
    alloc_block();
  }
}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  pthread_mutex_init(&inodemapLock,NULL);  
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  pthread_mutex_lock(&inodemapLock); 
  uint32_t inum = Alloc_inode(type);
  pthread_mutex_unlock(&inodemapLock); 
  return inum;
}
uint32_t
inode_manager::Alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  char tmp[BLOCK_SIZE];
  for(int i = 1; i <= INODE_NUM; ++i) 
  {
    bm->read_block(IBLOCK(i, BLOCK_NUM), tmp);
    struct inode* ino = (struct inode *)tmp + (i - 1) % IPB;
    if(ino->type == 0)
    {
      unsigned int timeNow = (unsigned int)time(NULL);
      ino->type = type;
      ino->atime = timeNow;
      ino->mtime = timeNow;
      ino->ctime = timeNow;
      for(index = 0; index <= NDIRECT; ++ index)
      {
        ino->blocks[index] = 0;
      }
      bm->write_block(IBLOCK(i,BLOCK_NUM), tmp);
      return i;
    }
  }
  printf("\tim: error! no free space anymore.\n");
  exit(0);
}

void
inode_manager::free_inode(uint32_t inum)
{
  pthread_mutex_lock(&inodemapLock); 
  Free_inode(inum);
  pthread_mutex_unlock(&inodemapLock);
  return;
}
void
inode_manager::Free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  char tmp[BLOCK_SIZE];
  bm->read_block(IBLOCK(inum, BLOCK_NUM), tmp);
  struct inode* inode = (struct inode*)(tmp) + (inum - 1)% IPB;

  //if the inode has been already freed, just return
  if(inode->type == 0)
  {
    return;
  }
  else
  {
    inode->type = 0;
    inode->size = 0;
    for(int i = 0; i <= NDIRECT; ++i)
    {
      inode->blocks[i] = 0;
    }
    put_inode(inum, inode);
  }
  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + (inum - 1) % IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + (inum - 1) % IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

void printBlocks(std::vector<blockid_t> blocks, uint32_t inum)
{
  printf("inum: %d:", inum);
  for(int i = 0; i < blocks.size(); ++i)
  {
    printf("%d,", blocks[i]);
  }
  printf("\n");
}
/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
vector<blockid_t> getBlockOfInode(struct inode *inode, block_manager *bm)
{
  int fileSize = inode->size;
  int index = 0;
  vector<blockid_t> result;
  while(fileSize > 0 && inode->blocks[index] != 0 && index < NDIRECT)
  {
    result.push_back(inode->blocks[index]);
    fileSize -= BLOCK_SIZE;
    index++;
  }
  if (index == NDIRECT && fileSize > 0)
  {
    char indirectBlock[BLOCK_SIZE];
    bm->read_block(inode->blocks[index], indirectBlock);
    blockid_t* indirectBlockList = (blockid_t*)(indirectBlock);
    int indirectIndex = 0;
    while(fileSize > 0 && indirectBlockList[indirectIndex] != 0 && (unsigned)indirectIndex < NINDIRECT)
    {
      result.push_back(indirectBlockList[indirectIndex]);
      fileSize -= BLOCK_SIZE;
      indirectIndex++;
    } 
  }
  return result;
}

void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
  char tmp[BLOCK_SIZE];
  struct inode* inode = get_inode(inum);
  vector<blockid_t> blocks = getBlockOfInode(inode, bm);
  int fileSize = inode->size;
  printf("want to read a file inode %d, size %d\n", inum, fileSize);
  *buf_out = (char *)malloc(fileSize);
  for(unsigned int i = 0; i<blocks.size();i++)
  {
    blockid_t bnum = blocks[i];
    bm->read_block(bnum, tmp);
    if(fileSize >= BLOCK_SIZE)
    {
      memcpy(*buf_out + i * BLOCK_SIZE, tmp, BLOCK_SIZE);
      fileSize -= BLOCK_SIZE;
    }
    else
    {
      memcpy(*buf_out + i * BLOCK_SIZE, tmp, fileSize);
      break;
    }
  }
  *size = inode->size;
  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */

  //read the inode
  char tmp[BLOCK_SIZE];
  struct inode* t = get_inode(inum);

  //free all the blocks of old file(including indirect inode block)
  vector<blockid_t> blocks = getBlockOfInode(t, bm);

  for(unsigned int i = 0; i<blocks.size();i++)
  {
    bm->free_block(blocks[i]);
  }
  if(t->blocks[NDIRECT] != 0)
  {
    bm->free_block(t->blocks[NDIRECT]);
  }
  for(int i = 0; i <= NDIRECT; ++i)
  {
    t->blocks[i] = 0;
  }
  t->size = size;

  //write the new file to the inode(now the inode is empty)
  int index = 0;
  int indirectIndex = 0;
  while(size >0)
  {
    if(index != NDIRECT)
    {
      blockid_t block = bm->alloc_block();
      t->blocks[index] = block;
      ++index;
      if(size > BLOCK_SIZE)
      {
        bm->write_block(block, buf);
        size -= BLOCK_SIZE;
        buf += BLOCK_SIZE;
      }
      else
      {
        char contentTail[BLOCK_SIZE];
        memcpy(contentTail, buf, size);
        bm->write_block(block, (const char*)contentTail);
        t->mtime = (unsigned int)time(NULL);
        t->ctime = (unsigned int)time(NULL);
        put_inode(inum, t);
        return;
      }
    }
    else
    {
      if(indirectIndex == 0)
      {
        blockid_t block = bm->alloc_block();
        char emptyBuf = char[BLOCK_SIZE];
        memset(blocks[block], 0, BLOCKS_SIZE);
        t->blocks[NDIRECT] = block;
      }
      bm->read_block(t->blocks[NDIRECT], tmp);
      blockid_t *indirectBlocks = (blockid_t*)(tmp);
      blockid_t dataBlock = bm->alloc_block();
      indirectBlocks[indirectIndex] = dataBlock;
      ++indirectIndex;
      
      bm->write_block(t->blocks[NDIRECT], (char*)indirectBlocks);

      if(size > BLOCK_SIZE)
      {
        bm->write_block(dataBlock, buf);
        size -= BLOCK_SIZE;
        buf += BLOCK_SIZE;
      }
      else
      {
        char contentTail[BLOCK_SIZE];
        memcpy(contentTail, buf, size);
        bm->write_block(dataBlock, (const char*)contentTail);
        t->mtime = (unsigned int)time(NULL);
        t->ctime = (unsigned int)time(NULL);
        put_inode(inum, t);
        return;
      }
    }
  }

  //in case of that the file size is 0
  t->mtime = (unsigned int)time(NULL);
  t->ctime = (unsigned int)time(NULL);
  put_inode(inum, t);
  return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  char tmp[BLOCK_SIZE];
  bm->read_block(IBLOCK(inum, BLOCK_NUM), tmp);
  struct inode* t = (struct inode*)(tmp) + (inum - 1) % IPB;
  if(t -> type == 0)
  {
    printf("\tim:inode is NULL!\n");
    return;
  }
  else
  {
    a.type = t->type;
    a.size = t->size;
    a.atime = t->atime;
    a.mtime = t->mtime;
    a.ctime = t->ctime;
  }
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  char tmp[BLOCK_SIZE];
  bm->read_block(IBLOCK(inum, BLOCK_NUM), tmp);
  struct inode* t = (struct inode*)(tmp) + (inum - 1) % IPB;
  vector<blockid_t> results = getBlockOfInode(t, bm);
  for(unsigned int i = 0;i < results.size();i++)
  {
    bm->free_block(results[i]);
  }
  if(t->blocks[NDIRECT] != 0)
  {
    bm->free_block(t->blocks[NDIRECT]);
  }
  free_inode(inum);
  return;
}


std::list<blockid_t> inodeBlocks(struct inode *inode, block_manager *bm)
{
  int index = 0;
  std::list<blockid_t> result;
  while(inode->blocks[index] != 0 && index < NDIRECT)
  {
    result.push_back(inode->blocks[index]);
    index++;
  }
  if (index == NDIRECT && fileSize > 0)
  {
    char indirectBlock[BLOCK_SIZE];
    bm->read_block(inode->blocks[index], indirectBlock);
    blockid_t* indirectBlockList = (blockid_t*)(indirectBlock);
    int indirectIndex = 0;
    while(indirectBlockList[indirectIndex] != 0 && (unsigned)indirectIndex < NINDIRECT)
    {
      result.push_back(indirectBlockList[indirectIndex]);
      indirectIndex++;
    } 
  }
  return result;
}

void
inode_manager::append_block(uint32_t inum, blockid_t &bid)
{
  /*
   * your code goes here.
   */
  blockid_t newBlock = alloc_block();
  bid = newBlock;
  struct inode* t = get_inode(inum);
  std::list<blockid_t> blocks = inodeBlocks(inum, bm);
  int size = blocks.size();
  if(size < NDIRECT)
  {
    t->blocks[size] = bid;
    put_inode(inum, t);
  }
  else
  {
    char *indirect = char[BLOCK_SIZE];
    bm->read_block(t->blocks[NDIRECT], indirect);
    blockid_t *blocks = (blockid_t *)indirect;
    blocks[size - NDIRECT] = bid;
    bm->write_block(blocks[NDIRECT], indirect);
  }
  return;
}

void
inode_manager::get_block_ids(uint32_t inum, std::list<blockid_t> &block_ids)
{
  /*
   * your code goes here.
   */
  std::list<blockid_t> tmp = inodeBlocks(inum, bm);
  int size = tmp.size();
  for(int i = 0; i < size; ++i)
  {
    block_ids.push_back(tmp.front());
    tmp.pop_front();
  }
  block_ids.push_back(tmp->size);
  return;
}

void
inode_manager::read_block(blockid_t id, char buf[BLOCK_SIZE])
{
  /*
   * your code goes here.
   */
  bm->read_block(id, buf);
  return;
}

void
inode_manager::write_block(blockid_t id, const char buf[BLOCK_SIZE])
{
  /*
   * your code goes here.
   */
  bm->write_block(id, buf);
}

void
inode_manager::complete(uint32_t inum, uint32_t size)
{
  /*
   * your code goes here.
   */
  struct inode *t = get_inode(inum);
  t->ctime = (unsigned int)time(NULL);
  t->mtime = (unsigned int)time(NULL);
  t->size = size;
  return;
}
