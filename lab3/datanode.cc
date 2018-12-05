#include "datanode.h"
#include <arpa/inet.h>
#include "extent_client.h"
#include <unistd.h>
#include <algorithm>
#include "threader.h"

using namespace std;

int DataNode::init(const string &extent_dst, const string &namenode, const struct sockaddr_in *bindaddr) {
  ec = new extent_client(extent_dst);

  // Generate ID based on listen address
  id.set_ipaddr(inet_ntoa(bindaddr->sin_addr));
  id.set_hostname(GetHostname());
  id.set_datanodeuuid(GenerateUUID());
  id.set_xferport(ntohs(bindaddr->sin_port));
  id.set_infoport(0);
  id.set_ipcport(0);

  // Save namenode address and connect
  make_sockaddr(namenode.c_str(), &namenode_addr);
  if (!ConnectToNN()) {
    delete ec;
    ec = NULL;
    return -1;
  }

  // Register on namenode
  if (!RegisterOnNamenode()) {
    delete ec;
    ec = NULL;
    close(namenode_conn);
    namenode_conn = -1;
    return -1;
  }

  /* Add your initialization here */

  return 0;
}

bool DataNode::ReadBlock(blockid_t bid, uint64_t offset, uint64_t len, string &buf) {
  /* Your lab4 part 2 code */
  if(ec->read_block(bid, buf) != extent_protocol::OK)
  {
    printf("read block failed\n");
    return false;
  }
  if(offset + len > BLOCK_SIZE)
  {
    buf.resize(offset + len, '\0');
  }
  buf = buf.substr(offset, len);
  return true;
}

bool DataNode::WriteBlock(blockid_t bid, uint64_t offset, uint64_t len, const string &buf) {
  /* Your lab4 part 2 code */
  string raw;
  if(ec->read_block(bid, raw) != extent_protocol::OK)
  {
    printf("read block failed\n");
    return false;
  }
  string content = buf;
  content.resize(len);
  raw.replace(offset, len, content, 0, len);
  raw.resize(BLOCK_SIZE);
  if(ec->write_block(bid, raw.c_str()))
  {
    printf("write block failed\n");
    return false;
  }
  return true;
}

