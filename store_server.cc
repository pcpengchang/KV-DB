
#include "db_server.h"

int main() {
  muduo::net::EventLoop loop;
  muduo::net::InetAddress local_addr("0.0.0.0", 10000);
  DbServer db_server(&loop, local_addr);

  db_server.Start();
  loop.loop();

  return 0;
}