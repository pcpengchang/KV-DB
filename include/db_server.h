/**
 * @file db_server.h
 * @author pengchang
 * @brief 使用muduo提供的TcpServer类完成一个数据存储服务器

 */

#ifndef DB_SERVER_H
#define DB_SERVER_H
#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/TcpServer.h>

#include "database.h"
class DbServer
{
public:
  typedef std::vector<std::string> VecS;
  DbServer(muduo::net::EventLoop *loop,
           const muduo::net::InetAddress &localAddr);
  ~DbServer() {}

  /**
   * @brief 新连接建立回调
   */
  void OnConnection(const muduo::net::TcpConnectionPtr &);

  /**
   * @brief 数据到达回调
   * @param[in] ptr 标志一条Tcp连接
   * @param[out] buf 数据存放的用户缓冲区
   * @param[out] time 数据到达时间
   */
  void OnMessage(const muduo::net::TcpConnectionPtr &ptr,
                 muduo::net::Buffer *buf, muduo::Timestamp time);

  /**
   * @brief 开启服务器
   */
  void Start();

  /**
   * @brief rdb存储
   */
  void RdbSave();

private:
  // 数据分库的数目
  static const long kDefaultDbNum = 16;
  /**
   * @brief 数据库初始化
   * @details
   * 生成所有数据分库，并对当前数据分库从rdb文件进行数据恢复。
   */
  void InitDB();

  /**
   * @brief 从字符串中解析一行命令，并调用相应的命令回调函数处理
   * @details 比如""set key1 value1",解析完毕后，会将其存入一个VecS对象，
   * 值为{"set", "key1","value1"}，然后将该对象传给命令回调函数。
   * @param[in] msg OnMessage收到的数据，即一行命令，比如"set key1 value1"
   * @return std::string 响应信息，比如"OK"
   */
  std::string ParseMsg(const std::string &msg);

  // 数据库操作命令的回调处理函数。目前只支持十八个命令。
  std::string SetCommand(VecS &&);
  std::string GetCommand(VecS &&);
  std::string PExpiredCommand(VecS &&);
  std::string ExpiredCommand(VecS &&);
  std::string BgsaveCommand(VecS &&);
  std::string SelectCommand(VecS &&);
  std::string RpushCommand(VecS &&);
  std::string RpopCommand(VecS &&);
  std::string HSetCommand(VecS &&);
  std::string HGetCommand(VecS &&);
  std::string HGetAllCommand(VecS &&);
  std::string SAddCommand(VecS &&);
  std::string SMembersCommand(VecS &&);
  std::string ZAddCommand(VecS &&);
  std::string ZCardCommand(VecS &&);
  std::string ZRangeCommand(VecS &&);
  std::string ZCountCommand(VecS &&);
  std::string ZGetAllCommand(VecS &&);

  std::string SaveHead();
  std::string SaveSelectDB(const int index);
  std::string SaveExpiredTime(const muduo::Timestamp &expiredTime);
  std::string SaveType(const int type);
  std::string SaveKV(const std::string &key, const std::string &value);
  /**
   * @brief 判断是否应执行RDB持久化
   * @return true 满足执行RDB持久化的条件
   * @return false 不满足执行RDB持久化的条件
   */
  bool CheckSaveCondition();

private:
  // db相关
  std::vector<std::unique_ptr<Database>> database_; // 所有数据库分库
  int db_idx_;                                      // 当前数据库分库编号
  std::unordered_map<std::string, std::function<std::string(VecS &&)>>
      cmd_dict_; // <命令名称, 命令回调函数对象>
  muduo::Timestamp last_save_;

  // net相关
  muduo::net::EventLoop *loop_;
  muduo::net::TcpServer server_;
};
#endif