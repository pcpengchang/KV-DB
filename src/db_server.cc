#include "db_server.h"

#include <muduo/base/Logging.h>
#include <unistd.h>

#include <cfloat>
#include <fstream>
#include <sstream>

#include "db_obj.h"
#include "db_status.h"
static const int kMicroSecondsPerSecond = 1000 * 1000;
static const int kMilliSecondsPerSecond = 1000;
static const int kMicroSecondsPerMilliSecond = 1000;

DbServer::DbServer(muduo::net::EventLoop *loop,
                   const muduo::net::InetAddress &localAddr)
    : loop_(loop),
      server_(loop_, localAddr, "DbServer"),
      last_save_(Timestamp::invalid())
{
  server_.setConnectionCallback(
      std::bind(&DbServer::OnConnection, this, std::placeholders::_1));
  server_.setMessageCallback(
      std::bind(&DbServer::OnMessage, this, std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3));
  // 绑定命令处理函数
  cmd_dict_.insert(std::make_pair(
      "set", std::bind(&DbServer::SetCommand, this, std::placeholders::_1)));
  cmd_dict_.insert(std::make_pair(
      "get", std::bind(&DbServer::GetCommand, this, std::placeholders::_1)));
  cmd_dict_.insert(std::make_pair(
      "pexpire",
      std::bind(&DbServer::PExpiredCommand, this, std::placeholders::_1)));
  cmd_dict_.insert(std::make_pair(
      "expire",
      std::bind(&DbServer::ExpiredCommand, this, std::placeholders::_1)));
  cmd_dict_.insert(std::make_pair(
      "bgsave",
      std::bind(&DbServer::BgsaveCommand, this, std::placeholders::_1)));
  cmd_dict_.insert(std::make_pair(
      "select",
      std::bind(&DbServer::SelectCommand, this, std::placeholders::_1)));
  cmd_dict_.insert(std::make_pair(
      "rpush",
      std::bind(&DbServer::RpushCommand, this, std::placeholders::_1)));
  cmd_dict_.insert(std::make_pair(
      "rpop", std::bind(&DbServer::RpopCommand, this, std::placeholders::_1)));
  cmd_dict_.insert(std::make_pair(
      "hset", std::bind(&DbServer::HSetCommand, this, std::placeholders::_1)));
  cmd_dict_.insert(std::make_pair(
      "hget", std::bind(&DbServer::HGetCommand, this, std::placeholders::_1)));
  cmd_dict_.insert(std::make_pair(
      "hgetall",
      std::bind(&DbServer::HGetAllCommand, this, std::placeholders::_1)));
  cmd_dict_.insert(std::make_pair(
      "sadd", std::bind(&DbServer::SAddCommand, this, std::placeholders::_1)));
  cmd_dict_.insert(std::make_pair(
      "smembers",
      std::bind(&DbServer::SMembersCommand, this, std::placeholders::_1)));
  cmd_dict_.insert(std::make_pair(
      "zadd", std::bind(&DbServer::ZAddCommand, this, std::placeholders::_1)));
  cmd_dict_.insert(std::make_pair(
      "zcard",
      std::bind(&DbServer::ZCardCommand, this, std::placeholders::_1)));
  cmd_dict_.insert(std::make_pair(
      "zrange",
      std::bind(&DbServer::ZRangeCommand, this, std::placeholders::_1)));
  cmd_dict_.insert(std::make_pair(
      "zcount",
      std::bind(&DbServer::ZCountCommand, this, std::placeholders::_1)));
  cmd_dict_.insert(std::make_pair(
      "zgetall",
      std::bind(&DbServer::ZGetAllCommand, this, std::placeholders::_1)));
  InitDB();
}

void DbServer::InitDB()
{
  // 初始化16个库
  for (int i = 0; i < kDefaultDbNum; ++i)
  {
    database_.emplace_back(std::make_unique<Database>());
  }
  db_idx_ = 0;
  database_[db_idx_]->RdbLoad(db_idx_);
}

void DbServer::OnConnection(const muduo::net::TcpConnectionPtr &conn)
{
  LOG_INFO << "StoreServer - " << conn->peerAddress().toIpPort() << " -> "
           << conn->localAddress().toIpPort() << " is "
           << (conn->connected() ? "UP" : "DOWN");
}

void DbServer::OnMessage(const muduo::net::TcpConnectionPtr &conn,
                         muduo::net::Buffer *buf, muduo::Timestamp timestamp)
{
  auto msg = buf->retrieveAllAsString();
  auto res = ParseMsg(msg);
  conn->send(res);
}

void DbServer::Start()
{
  server_.start();

  LOG_INFO << "StoreServer started at " << server_.ipPort();
}

void DbServer::RdbSave()
{
  pid_t pid = fork();
  if (pid == 0)
  {
    LOG_INFO << "this is child process";

    char buf[1024]{0};
    std::string path = getcwd(buf, 1024);
    assert(!path.empty());
    path += "/dump.rdb";

    std::ofstream out;
    out.open(path, std::ios::out | std::ios::trunc | std::ios::binary);
    LOG_INFO << "rdb文件路径:" << path;
    if (!out.is_open())
    {
      LOG_FATAL << "RDB持久化失败...";
    }

    std::string str;
    // 存储RDB头
    str = SaveHead();
    for (int i = 0; i < kDefaultDbNum; ++i)
    {
      if (database_[i]->GetKeySize() == 0)
      {
        continue;
      }
      str += SaveSelectDB(i);
      // String
      if (database_[i]->GetKeyStringSize() != 0)
      {
        str += SaveType(dbobject::kDbString);
        auto obj = database_[i]->GetKeyStringObj();
        for (auto it = obj.begin(); it != obj.end(); it++)
        {
          str += SaveExpiredTime(
              database_[i]->GetKeyExpiredTime(dbobject::kDbString, it->first));
          str += SaveKV(it->first, it->second);
        }
      }
      // List
      if (database_[i]->GetKeyListSize() != 0)
      {
        str += SaveType(dbobject::kDbList);
        auto obj = database_[i]->GetKeyListObj();
        for (auto it = obj.begin(); it != obj.end(); it++)
        {
          str += SaveExpiredTime(
              database_[i]->GetKeyExpiredTime(dbobject::kDbList, it->first));
          auto iter = it->second.begin();
          std::string tmp = '!' + std::to_string(it->second.size());
          for (; iter != it->second.end(); iter++)
          {
            tmp += '!' + std::to_string(iter->size()) + '$' + iter->c_str();
          }
          str += '!' + std::to_string(it->first.size()) + '#' +
                 it->first.c_str() + tmp;
        }
      }
      // Hash
      if (database_[i]->GetKeyHashSize() != 0)
      {
        str += SaveType(dbobject::kDbHash);
        auto obj = database_[i]->GetKeyHashObj();
        auto it = obj.begin();
        for (; it != obj.end(); it++)
        {
          str += SaveExpiredTime(
              database_[i]->GetKeyExpiredTime(dbobject::kDbHash, it->first));
          auto iter = it->second.begin();
          std::string tmp = '!' + std::to_string(it->second.size());
          for (; iter != it->second.end(); iter++)
          {
            tmp += SaveKV(iter->first, iter->second);
          }
          str += '!' + std::to_string(it->first.size()) + '#' +
                 it->first.c_str() + tmp;
        }
      }
      // Set
      if (database_[i]->GetKeySetSize() != 0)
      {
        str += SaveType(dbobject::kDbSet);
        auto it = database_[i]->GetKeySetObj().begin();
        for (; it != database_[i]->GetKeySetObj().end(); it++)
        {
          str += SaveExpiredTime(
              database_[i]->GetKeyExpiredTime(dbobject::kDbSet, it->first));
          auto iter = it->second.begin();
          std::string tmp = '!' + std::to_string(it->second.size());
          for (; iter != it->second.end(); iter++)
          {
            tmp += '!' + std::to_string(iter->size()) + '$' + iter->c_str();
          }
          str += '!' + std::to_string(it->first.size()) + '#' +
                 it->first.c_str() + tmp;
        }
      }
      // ZSet
      if (database_[i]->GetKeyZSetSize() != 0)
      {
        str += SaveType(dbobject::kDbZSet);
        auto it = database_[i]->GetKeyZSetObj().begin();
        for (; it != database_[i]->GetKeyZSetObj().end(); it++)
        {
          str += SaveExpiredTime(
              database_[i]->GetKeyExpiredTime(dbobject::kDbZSet, it->first));
          std::string tmp = '!' + std::to_string(it->second->GetLength());
          RangeSpec spec(DBL_MIN, DBL_MAX);
          std::vector<SkiplistNode *> vecSkip(it->second->GetNodeInRange(spec));
          for (int j = 0; j < vecSkip.size(); j++)
          {
            tmp += SaveKV(vecSkip[j]->obj_, std::to_string(vecSkip[j]->score_));
          }
          str += '!' + std::to_string(it->first.size()) + '#' +
                 it->first.c_str() + tmp;
        }
      }
      str.append("EOF");
      out.write(str.c_str(), str.size());
    }
    out.close();
    exit(0);
  }
  else if (pid > 0)
  {
    LOG_INFO << "this is parent process, the child process pid is " << pid;
  }
  else
  {
    LOG_ERROR << "fork error";
  }
}

// 解析命令
std::string DbServer::ParseMsg(const std::string &msg)
{
  std::string res;
  std::istringstream ss(msg);
  std::string cmd, key, objKey, objValue;

  ss >> cmd;
  if (cmd.empty())
  {
    return DbStatus::NotFound(" ").ToString();
  }
  if (cmd == "set")
  {
    auto it = cmd_dict_.find(cmd);
    if (it == cmd_dict_.end())
    {
      return DbStatus::NotFound("command").ToString();
    }
    else
    {
      ss >> key;
      ss >> objKey;
      VecS vs = {cmd, key, objKey};
      res = it->second(std::move(vs));
    }
  }
  else if (cmd == "get")
  {
    auto it = cmd_dict_.find(cmd);
    if (it == cmd_dict_.end())
    {
      return DbStatus::NotFound("command").ToString();
    }
    else
    {
      ss >> key;
      VecS vs = {cmd, key};
      res = it->second(std::move(vs));
    }
  }
  else if (cmd == "pexpire")
  {
    auto it = cmd_dict_.find(cmd);
    if (it == cmd_dict_.end())
    {
      return DbStatus::NotFound("command").ToString();
    }
    else
    {
      ss >> key;
      ss >> objKey;
      VecS vs = {cmd, key, objKey};
      res = it->second(std::move(vs));
    }
  }
  else if (cmd == "expire")
  {
    auto it = cmd_dict_.find(cmd);
    if (it == cmd_dict_.end())
    {
      return DbStatus::NotFound("command").ToString();
    }
    else
    {
      ss >> key;
      ss >> objKey;
      VecS vs = {cmd, key, objKey};
      res = it->second(std::move(vs));
    }
  }
  else if (cmd == "bgsave")
  {
    auto it = cmd_dict_.find(cmd);
    if (it == cmd_dict_.end())
    {
      return DbStatus::NotFound("command").ToString();
    }
    else
    {
      VecS vs = {cmd};
      res = it->second(std::move(vs));
    }
  }
  else if (cmd == "select")
  {
    auto it = cmd_dict_.find(cmd);
    if (it == cmd_dict_.end())
    {
      return DbStatus::NotFound("command").ToString();
    }
    else
    {
      ss >> key;
      VecS vs = {cmd, key};
      res = it->second(std::move(vs));
    }
  }
  else if (cmd == "rpush")
  {
    auto it = cmd_dict_.find(cmd);
    if (it == cmd_dict_.end())
    {
      return DbStatus::NotFound("command").ToString();
    }
    else
    {
      ss >> key;
      while (ss >> objKey)
      {
        VecS vs = {cmd, key, objKey};
        res = it->second(std::move(vs));
      }
    }
  }
  else if (cmd == "rpop")
  {
    auto it = cmd_dict_.find(cmd);
    if (it == cmd_dict_.end())
    {
      return DbStatus::NotFound("command").ToString();
    }
    else
    {
      ss >> key;
      VecS vs = {cmd, key};
      res = it->second(std::move(vs));
    }
  }
  else if (cmd == "hset")
  {
    auto it = cmd_dict_.find(cmd);
    if (it == cmd_dict_.end())
    {
      return DbStatus::NotFound("command").ToString();
    }
    else
    {
      ss >> key;
      ss >> objKey;
      ss >> objValue;
      VecS vs = {cmd, key, objKey, objValue};
      res = it->second(std::move(vs));
    }
  }
  else if (cmd == "hget")
  {
    auto it = cmd_dict_.find(cmd);
    if (it == cmd_dict_.end())
    {
      return DbStatus::NotFound("command").ToString();
    }
    else
    {
      ss >> key;
      ss >> objKey;
      VecS vs = {cmd, key, objKey};
      res = it->second(std::move(vs));
    }
  }
  else if (cmd == "hgetall")
  {
    auto it = cmd_dict_.find(cmd);
    if (it == cmd_dict_.end())
    {
      return DbStatus::NotFound("command").ToString();
    }
    else
    {
      ss >> key;
      VecS vs = {cmd, key};
      res = it->second(std::move(vs));
    }
  }
  else if (cmd == "sadd")
  {
    auto it = cmd_dict_.find(cmd);
    if (it == cmd_dict_.end())
    {
      return DbStatus::NotFound("command").ToString();
    }
    else
    {
      ss >> key;
      ss >> objKey;
      VecS vs = {cmd, key, objKey};
      res = it->second(std::move(vs));
    }
  }
  else if (cmd == "smembers")
  {
    auto it = cmd_dict_.find(cmd);
    if (it == cmd_dict_.end())
    {
      return DbStatus::NotFound("command").ToString();
    }
    else
    {
      ss >> key;
      VecS vs = {cmd, key};
      res = it->second(std::move(vs));
    }
  }
  else if (cmd == "zadd")
  {
    auto it = cmd_dict_.find(cmd);
    if (it == cmd_dict_.end())
    {
      return DbStatus::NotFound("command").ToString();
    }
    else
    {
      ss >> key;
      ss >> objKey;
      ss >> objValue;
      VecS vs = {cmd, key, objKey, objValue};
      res = it->second(std::move(vs));
    }
  }
  else if (cmd == "zcard")
  {
    auto it = cmd_dict_.find(cmd);
    if (it == cmd_dict_.end())
    {
      return DbStatus::NotFound("command").ToString();
    }
    else
    {
      ss >> key;
      VecS vs = {cmd, key};
      res = it->second(std::move(vs));
    }
  }
  else if (cmd == "zrange")
  {
    auto it = cmd_dict_.find(cmd);
    if (it == cmd_dict_.end())
    {
      return DbStatus::NotFound("command").ToString();
    }
    else
    {
      ss >> key;
      ss >> objKey;   // range Start
      ss >> objValue; // range end
      VecS vs = {cmd, key, objKey, objValue};
      res = it->second(std::move(vs));
    }
  }
  else if (cmd == "zcount")
  {
    auto it = cmd_dict_.find(cmd);
    if (it == cmd_dict_.end())
    {
      return DbStatus::NotFound("command").ToString();
    }
    else
    {
      ss >> key;
      ss >> objKey;   // range Start
      ss >> objValue; // range end
      VecS vs = {cmd, key, objKey, objValue};
      res = it->second(std::move(vs));
    }
  }
  else if (cmd == "zgetall")
  {
    auto it = cmd_dict_.find(cmd);
    if (it == cmd_dict_.end())
    {
      return DbStatus::NotFound("command").ToString();
    }
    else
    {
      ss >> key;
      VecS vs = {cmd, key};
      res = it->second(std::move(vs));
    }
  }
  else
  {
    return DbStatus::NotFound("command").ToString();
  }
  return res;
}

std::string DbServer::SetCommand(VecS &&argv)
{
  if (argv.size() != 3)
  {
    return DbStatus::IOError("Parameter error").ToString();
  }
  // 处理过期时间
  bool expired =
      database_[db_idx_]->JudgeKeyExpiredTime(dbobject::kDbString, argv[1]);
  if (expired)
  {
    database_[db_idx_]->DelKey(dbobject::kDbString, argv[1]);
  }

  bool res = database_[db_idx_]->AddKey(dbobject::kDbString, argv[1], argv[2],
                                        dbobject::kDefaultObjValue);

  return res ? DbStatus::Ok().ToString()
             : DbStatus::IOError("set error").ToString();
}

std::string DbServer::GetCommand(VecS &&argv)
{
  if (argv.size() != 2)
  {
    return DbStatus::IOError("Parameter error").ToString();
  }
  // 处理过期时间
  // TODO: 删除策略， 是不是应该放到database里？
  // bool expired =
  //     database_[db_idx_]->JudgeKeyExpiredTime(dbobject::kDbString, argv[1]);
  // if (expired) {
  //   database_[db_idx_]->DelKey(dbobject::kDbString, argv[1]);
  //   return DbStatus::IOError("Empty Content").ToString();
  // }

  std::string res = database_[db_idx_]->GetKey(dbobject::kDbString, argv[1]);

  return res.empty() ? DbStatus::IOError("Empty Content").ToString() : res;
}

std::string DbServer::PExpiredCommand(VecS &&argv)
{
  if (argv.size() != 3)
  {
    return DbStatus::IOError("Parameter error").ToString();
  }
  // kDbString
  bool res = database_[db_idx_]->SetPExpireTime(dbobject::kDbString, argv[1],
                                                atof(argv[2].c_str()));
  if (!res)
  {
    // kDbList
    res = database_[db_idx_]->SetPExpireTime(dbobject::kDbList, argv[1],
                                             atof(argv[2].c_str()));
  }
  if (!res)
  {
    // kDbHash
    res = database_[db_idx_]->SetPExpireTime(dbobject::kDbHash, argv[1],
                                             atof(argv[2].c_str()));
  }
  if (!res)
  {
    // kDbSet
    res = database_[db_idx_]->SetPExpireTime(dbobject::kDbSet, argv[1],
                                             atof(argv[2].c_str()));
  }
  if (!res)
  {
    // kDbZSet
    res = database_[db_idx_]->SetPExpireTime(dbobject::kDbZSet, argv[1],
                                             atof(argv[2].c_str()));
  }

  return res ? DbStatus::Ok().ToString()
             : DbStatus::IOError("pExpire error").ToString();
}

std::string DbServer::ExpiredCommand(VecS &&argv)
{
  if (argv.size() != 3)
  {
    return DbStatus::IOError("Parameter error").ToString();
  }
  // kDbString
  bool res = database_[db_idx_]->SetPExpireTime(
      dbobject::kDbString, argv[1],
      atof(argv[2].c_str()) * kMicroSecondsPerMilliSecond);
  if (!res)
  {
    // kDbList
    res = database_[db_idx_]->SetPExpireTime(
        dbobject::kDbList, argv[1],
        atof(argv[2].c_str()) * kMicroSecondsPerMilliSecond);
  }
  if (!res)
  {
    // kDbHash
    res = database_[db_idx_]->SetPExpireTime(
        dbobject::kDbHash, argv[1],
        atof(argv[2].c_str()) * kMicroSecondsPerMilliSecond);
  }
  if (!res)
  {
    // kDbSet
    res = database_[db_idx_]->SetPExpireTime(
        dbobject::kDbSet, argv[1],
        atof(argv[2].c_str()) * kMicroSecondsPerMilliSecond);
  }
  if (!res)
  {
    // kDbZSet
    res = database_[db_idx_]->SetPExpireTime(
        dbobject::kDbZSet, argv[1],
        atof(argv[2].c_str()) * kMicroSecondsPerMilliSecond);
  }
  return res ? DbStatus::Ok().ToString()
             : DbStatus::IOError("expire error").ToString();
}

std::string DbServer::BgsaveCommand(VecS &&argv)
{
  if (argv.size() != 1)
  {
    return DbStatus::IOError("Parameter error").ToString();
  }
  bool res = CheckSaveCondition();
  return res ? DbStatus::Ok().ToString()
             : DbStatus::IOError("bgsave error").ToString();
}

std::string DbServer::SelectCommand(VecS &&argv)
{
  if (argv.size() != 2)
  {
    return DbStatus::IOError("Parameter error").ToString();
  }
  int idx = atoi(argv[1].c_str());
  db_idx_ = idx - 1;
  database_[db_idx_]->RdbLoad(db_idx_); // 加载rdb文件
  return DbStatus::Ok().ToString();
}

std::string DbServer::RpushCommand(VecS &&argv)
{
  if (argv.size() < 3)
  {
    return DbStatus::IOError("Parameter error").ToString();
  }
  int flag;
  for (int i = 2; i < argv.size(); i++)
  {
    flag = database_[db_idx_]->AddKey(dbobject::kDbList, argv[1], argv[i],
                                      dbobject::kDefaultObjValue);
  }

  return flag ? DbStatus::Ok().ToString()
              : DbStatus::IOError("rpush error").ToString();
}

std::string DbServer::RpopCommand(VecS &&argv)
{
  if (argv.size() != 2)
  {
    return DbStatus::IOError("Parameter error").ToString();
  }
  // 处理过期时间
  bool expired =
      database_[db_idx_]->JudgeKeyExpiredTime(dbobject::kDbList, argv[1]);
  if (expired)
  {
    database_[db_idx_]->DelKey(dbobject::kDbList, argv[1]);
    return DbStatus::IOError("Empty Content").ToString();
  }

  std::string res = database_[db_idx_]->RPopList(argv[1]);
  if (res.empty())
  {
    return DbStatus::IOError("rpop error").ToString();
  }
  else
  {
    return res;
  }
}

std::string DbServer::HSetCommand(VecS &&argv)
{
  if (argv.size() != 4)
  {
    return DbStatus::IOError("Parameter error").ToString();
  }
  bool flag =
      database_[db_idx_]->AddKey(dbobject::kDbHash, argv[1], argv[2], argv[3]);

  return flag ? DbStatus::Ok().ToString()
              : DbStatus::IOError("hset error").ToString();
}

std::string DbServer::HGetCommand(VecS &&argv)
{
  if (argv.size() != 3)
  {
    return DbStatus::IOError("Parameter error").ToString();
  }
  auto tmp = database_[db_idx_]->GetKeyHashObj();
  auto it = tmp.find(argv[1]);
  if (it == tmp.end())
  {
    return DbStatus::NotFound("Empty Content").ToString();
  }
  else
  {
    auto iter = it->second.find(argv[2]);
    if (iter == it->second.end())
    {
      return DbStatus::NotFound("Empty Content").ToString();
    }
    else
    {
      return iter->second;
    }
  }
}

std::string DbServer::HGetAllCommand(VecS &&argv)
{
  if (argv.size() != 2)
  {
    return DbStatus::IOError("Parameter error").ToString();
  }
  std::string res = database_[db_idx_]->GetKey(dbobject::kDbHash, argv[1]);

  return res.empty() ? DbStatus::IOError("Empty Content").ToString() : res;
}

std::string DbServer::SAddCommand(VecS &&argv)
{
  if (argv.size() != 3)
  {
    return DbStatus::IOError("Parameter error").ToString();
  }
  bool flag = database_[db_idx_]->AddKey(dbobject::kDbSet, argv[1], argv[2],
                                         dbobject::kDefaultObjValue);

  return flag ? DbStatus::Ok().ToString()
              : DbStatus::IOError("sadd error").ToString();
}

std::string DbServer::SMembersCommand(VecS &&argv)
{
  if (argv.size() != 2)
  {
    return DbStatus::IOError("Parameter error").ToString();
  }
  std::string res = database_[db_idx_]->GetKey(dbobject::kDbSet, argv[1]);

  return res.empty() ? DbStatus::NotFound("Empty Content").ToString() : res;
}

std::string DbServer::ZAddCommand(VecS &&argv)
{
  if (argv.size() != 4)
  {
    return DbStatus::IOError("Parameter error").ToString();
  }
  bool flag =
      database_[db_idx_]->AddKey(dbobject::kDbZSet, argv[1], argv[2], argv[3]);

  return flag ? DbStatus::Ok().ToString()
              : DbStatus::IOError("zadd error").ToString();
}

std::string DbServer::ZCardCommand(VecS &&argv)
{
  if (argv.size() != 2)
  {
    return DbStatus::IOError("Parameter error").ToString();
  }
  auto tmpZset = database_[db_idx_]->GetKeyZSetObj();
  auto it = tmpZset.find(argv[1]);
  if (it == tmpZset.end())
  {
    return DbStatus::NotFound("key").ToString();
  }
  else
  {
    return std::to_string(it->second->GetLength());
  }
}

std::string DbServer::ZRangeCommand(VecS &&argv)
{
  if (argv.size() != 4 || argv[2].empty() || argv[3].empty())
  {
    return DbStatus::IOError("Parameter error").ToString();
  }
  std::string res;
  // zset的key
  std::string args = argv[1];
  // 添加range的范围
  args += ':' + argv[2] + '@' + argv[3];
  res = database_[db_idx_]->GetKey(dbobject::kDbZSet, args);

  return res.empty() ? DbStatus::NotFound("Empty Content").ToString() : res;
}

std::string DbServer::ZCountCommand(VecS &&argv)
{
  if (argv.size() != 4 || argv[2].empty() || argv[3].empty())
  {
    return DbStatus::IOError("Parameter error").ToString();
  }
  std::string res;
  auto it = database_[db_idx_]->GetKeyZSetObj();
  RangeSpec range(std::stod(argv[2]), std::stod(argv[3]));
  auto obj = it.find(argv[1]);
  if (obj == it.end())
  {
    res = DbStatus::NotFound("key").ToString();
  }
  else
  {
    res = "(count)" + std::to_string(obj->second->GetCountInRange(range));
  }
  return res;
}

std::string DbServer::ZGetAllCommand(VecS &&argv)
{
  if (argv.size() != 2)
  {
    return DbStatus::IOError("Parameter error").ToString();
  }
  std::string res = database_[db_idx_]->GetKey(dbobject::kDbZSet, argv[1]);

  return res.empty() ? DbStatus::NotFound("Empty Content").ToString() : res;
}
std::string DbServer::SaveHead()
{
  std::string tmp = "KV0001";
  return tmp;
}

std::string DbServer::SaveSelectDB(const int index)
{
  return "SD" + std::to_string(index);
}

std::string DbServer::SaveExpiredTime(const Timestamp &expiredTime)
{
  return "ST" + std::to_string(expiredTime.microSecondsSinceEpoch());
}

std::string DbServer::SaveType(const int type)
{
  return "^" + std::to_string(type);
}

std::string DbServer::SaveKV(const std::string &key, const std::string &value)
{
  char buf[1024];
  sprintf(buf, "!%d#%s!%d$%s", static_cast<int>(key.size()), key.c_str(),
          static_cast<int>(value.size()), value.c_str());
  return std::string(buf);
}

bool DbServer::CheckSaveCondition()
{
  Timestamp save_interval(Timestamp::now().microSecondsSinceEpoch() -
                          last_save_.microSecondsSinceEpoch());
  if (save_interval > dbobject::kRdbDefaultTime)
  {
    LOG_INFO << "bgsaving...";
    RdbSave();
    last_save_ = Timestamp::now();
    return true;
  }
  return false;
}