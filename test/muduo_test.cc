#include <muduo/base/Logging.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpServer.h>

#include <boost/bind.hpp>

// 使用muduo网络库完成回显服务器
class EchoServer {
 public:
  EchoServer(muduo::net::EventLoop* loop,
             const muduo::net::InetAddress& listenAddr);

  void Start();

 private:
  void OnConnection(const muduo::net::TcpConnectionPtr& conn);

  void OnMessage(const muduo::net::TcpConnectionPtr& conn,
                 muduo::net::Buffer* buf, muduo::Timestamp time);

  muduo::net::TcpServer server_;
};

EchoServer::EchoServer(muduo::net::EventLoop* loop,
                       const muduo::net::InetAddress& listenAddr)
    : server_(loop, listenAddr, "EchoServer") {
  server_.setConnectionCallback(
      boost::bind(&EchoServer::OnConnection, this, _1));
  server_.setMessageCallback(
      boost::bind(&EchoServer::OnMessage, this, _1, _2, _3));
}

void EchoServer::Start() { server_.start(); }

void EchoServer::OnConnection(const muduo::net::TcpConnectionPtr& conn) {
  LOG_INFO << "EchoServer - " << conn->peerAddress().toIpPort() << " -> "
           << conn->localAddress().toIpPort() << " is "
           << (conn->connected() ? "UP" : "DOWN");
}

void EchoServer::OnMessage(const muduo::net::TcpConnectionPtr& conn,
                           muduo::net::Buffer* buf, muduo::Timestamp time) {
  // 接收到所有的消息，然后回显
  muduo::string msg(buf->retrieveAllAsString());
  LOG_INFO << conn->name() << " echo " << msg.size() << " bytes, "
           << "data received at " << time.toString();
  conn->send(msg);
}

int main() {
  LOG_INFO << "pid = " << getpid();
  muduo::net::EventLoop loop;
  muduo::net::InetAddress listenAddr(10005);
  EchoServer server(&loop, listenAddr);
  server.Start();
  loop.loop();
}
// compile: g++ muduo_test.cc -lmuduo_net -lmuduo_base -lpthread -std=c++11