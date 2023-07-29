#include "database.h"

#include <fcntl.h>
#include <muduo/base/Logging.h>
#include <muduo/net/EventLoop.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cfloat>
#include <iostream>

#include "db_obj.h"
#include "db_status.h"

static const int kMicroSecondsPerSecond = 1000 * 1000;
static const int kMilliSecondsPerSecond = 1000;
static const int kMicroSecondsPerMilliSecond = 1000;

/*
 * 随机从过期字典中抽出一部分key，检查是否到期，过期则删除。
 * 如果过期key达到一定比例则再次触发alarm信号
 */
void Database::DingqiHandler()
{
  int kCheckNum = dbobject::kCheckNum;
  int len1 = GetKeyStringSize();
  int len2 = GetKeyListSize();
  int len3 = GetKeyHashSize();
  int len4 = GetKeySetSize();
  int len5 = GetKeyZSetSize();
  int len = len1 + len2 + len3 + len4 + len5;
  if (len < kCheckNum)
    return;
  int del_cnt = 0;

  // 五种数据类型各自占比
  len1 = len1 / len * kCheckNum;
  len2 = len2 / len * kCheckNum;
  len3 = len3 / len * kCheckNum;
  len4 = len4 / len * kCheckNum;
  len5 = len5 / len * kCheckNum;

  //
  for (auto &t : string_)
  {
    auto key = t.first;
    if (JudgeKeyExpiredTime(dbobject::kDbString, key))
    {
      DelKey(dbobject::kDbString, key);
      ++del_cnt;
    }
    if ((--len1) == 0)
      break;
  }
  for (auto &t : list_)
  {
    auto key = t.first;
    if (JudgeKeyExpiredTime(dbobject::kDbList, key))
    {
      DelKey(dbobject::kDbList, key);
      ++del_cnt;
    }
    if ((--len2) == 0)
      break;
  }
  for (auto &t : hash_)
  {
    auto key = t.first;
    if (JudgeKeyExpiredTime(dbobject::kDbHash, key))
    {
      DelKey(dbobject::kDbHash, key);
      ++del_cnt;
    }
    if ((--len3) == 0)
      break;
  }
  for (auto &t : set_)
  {
    auto key = t.first;
    if (JudgeKeyExpiredTime(dbobject::kDbSet, key))
    {
      DelKey(dbobject::kDbSet, key);
      ++del_cnt;
    }
    if ((--len4) == 0)
      break;
  }
  for (auto &t : zset_)
  {
    auto key = t.first;
    if (JudgeKeyExpiredTime(dbobject::kDbZSet, key))
    {
      DelKey(dbobject::kDbZSet, key);
      ++del_cnt;
    }
    if ((--len5) == 0)
      break;
  }

  // 当过期key数量占抽查数量的一半时，立即再次触发定期删除
  if (del_cnt > kCheckNum / 2)
  {
    DingqiHandler();
  }
}

Database::Database()
{
  // 添加定期删除策略的定时任务
  int expire_del_time = 3; // 定期删除的时间
  muduo::net::EventLoop::getEventLoopOfCurrentThread()->runEvery(
      expire_del_time, std::bind(&Database::DingqiHandler, this));
}
// 惰性删除这里不用做任何事，只需要在查该key时判断一下过期没有即可，过期则删除
// 定时删除使用Muduo提供的定时器模块，在设置key过期时间时添加一个定时任务，时间到期就删除该key

void Database::RdbLoad(int index)
{
  char tmp[1024]{0};
  std::string path = getcwd(tmp, 1024);
  path += "/dump.rdb";
  int fd = open(path.c_str(), O_CREAT | O_RDONLY, 0644);
  assert(fd != -1);

  // 获取文件信息
  struct stat buf;
  fstat(fd, &buf);
  if (buf.st_size == 0)
    return;

  char *addr = static_cast<char *>(
      mmap(NULL, buf.st_size, PROT_READ, MAP_SHARED, fd, 0));
  if (addr == MAP_FAILED)
  {
    close(fd);
    LOG_FATAL << "RdbSave error";
  }
  close(fd);

  std::string data(addr, addr + buf.st_size);
  assert(munmap(addr, buf.st_size) != -1);

  int p1 = 0, p2 = 0;
  int dbIdx = 0;
  do
  {
    p1 = data.find("SD", p2);
    // 没有数据需要载入
    if (p1 == data.npos)
    {
      return;
    }
    p2 = data.find('^', p1);
    dbIdx = atoi(InterceptString(data, p1 + 2, p2).c_str());
  } while (dbIdx != index);

  int end = data.find("SD", p2);
  if (end == data.npos)
  {
    end = data.find("EOF");
  }

  while (p1 < end && p2 < end)
  {
    p2 = data.find('^', p1);
    p1 = data.find("ST", p2);
    int type = atoi(InterceptString(data, p2 + 1, p1).c_str());

    if (type == dbobject::kDbString)
    {
      do
      {
        p2 = data.find('!', p1);
        Timestamp expireTime(atoi(InterceptString(data, p1 + 2, p2).c_str()));
        p1 = data.find('#', p2);

        int keyLen = atoi(InterceptString(data, p2 + 1, p1).c_str());
        std::string key = data.substr(p1 + 1, keyLen);

        p2 = data.find('!', p1);
        p1 = data.find('$', p2);
        int valueLen = atoi(InterceptString(data, p2 + 1, p1).c_str());
        std::string value = data.substr(p1 + 1, valueLen);

        AddKey(dbobject::kDbString, key, value, dbobject::kDefaultObjValue);
        if (expireTime > Timestamp::now())
        {
          SetPExpireTime(dbobject::kDbString, key, expireTime);
        }
        p1 += valueLen + 1;
      } while (data.substr(p1, 2) == "ST");
      continue;
    }
    if (type == dbobject::kDbList)
    {
      do
      {
        p2 = data.find('!', p1);
        Timestamp expireTime(atoi(InterceptString(data, p1 + 2, p2).c_str()));
        p1 = data.find('#', p2);

        int keyLen = atoi(InterceptString(data, p2 + 1, p1).c_str());
        std::string key = data.substr(p1 + 1, keyLen);

        p2 = data.find('!', p1);
        p1 = data.find('$', p2);
        int valueSize = atoi(InterceptString(data, p2 + 1, p1).c_str());
        int valueLen = 0;
        while (valueSize--)
        {
          p2 = p1;
          p1 = data.find('$', p2);
          valueLen = atoi(InterceptString(data, p2 + 1, p1).c_str());
          std::string value = data.substr(p1 + 1, valueLen);
          if (valueSize > 1)
          {
            p1 = data.find('!', p2 + 1);
          }
          AddKey(dbobject::kDbList, key, value, dbobject::kDefaultObjValue);
        }
        if (expireTime > Timestamp::now())
        {
          SetPExpireTime(dbobject::kDbList, key, expireTime);
        }
        p1 += valueLen + 1;
      } while (data.substr(p1, 2) == "ST");
      continue;
    }
    if (type == dbobject::kDbHash)
    {
      do
      {
        p2 = data.find('!', p1);
        Timestamp expireTime(atoi(InterceptString(data, p1 + 2, p2).c_str()));
        p1 = data.find('#', p2);
        int keyLen = atoi(InterceptString(data, p2 + 1, p1).c_str());
        std::string key = data.substr(p1 + 1, keyLen);
        p2 = data.find('!', p1);
        p1 = data.find('!', p2 + 1);
        int valueSize = atoi(InterceptString(data, p2 + 1, p1).c_str());
        int valueLen = 0;
        while (valueSize--)
        {
          p2 = data.find('#', p1);
          int valueKeyLen = atoi(InterceptString(data, p1 + 1, p2).c_str());
          std::string valueKey = data.substr(p2 + 1, valueKeyLen);
          p1 = data.find('!', p2);
          p2 = p1;
          p1 = data.find('$', p2);
          valueLen = atoi(InterceptString(data, p2 + 1, p1).c_str());
          std::string value = data.substr(p1 + 1, valueLen);
          if (valueSize > 1)
          {
            p1 = data.find('!', p2 + 1);
          }
          AddKey(dbobject::kDbHash, key, valueKey, value);
        }
        if (expireTime > Timestamp::now())
        {
          SetPExpireTime(dbobject::kDbList, key, expireTime);
        }
        p1 += 1 + valueLen;
      } while (data.substr(p1, 2) == "ST");
      continue;
    }
    if (type == dbobject::kDbSet)
    {
      do
      {
        p2 = data.find('!', p1);
        Timestamp expireTime(atoi(InterceptString(data, p1 + 2, p2).c_str()));
        p1 = data.find('#', p2);
        int keyLen = atoi(InterceptString(data, p2 + 1, p1).c_str());
        std::string key = data.substr(p1 + 1, keyLen);
        p2 = data.find('!', p1);
        p1 = data.find('!', p2 + 1);
        int valueSize = atoi(InterceptString(data, p2 + 1, p1).c_str());
        int valueLen = 0;
        while (valueSize--)
        {
          p2 = p1;
          p1 = data.find('$', p2);
          valueLen = atoi(InterceptString(data, p2 + 1, p1).c_str());
          std::string value = data.substr(p1 + 1, valueLen);

          if (valueSize > 1)
          {
            p1 = data.find('!', p2 + 1);
          }
          AddKey(dbobject::kDbSet, key, value, dbobject::kDefaultObjValue);
        }
        if (expireTime > Timestamp::now())
        {
          SetPExpireTime(dbobject::kDbSet, key, expireTime);
        }
        p1 += 1 + valueLen;
      } while (data.substr(p1, 2) == "ST");
      continue;
    }
    if (type == dbobject::kDbZSet)
    {
      do
      {
        p2 = data.find('!', p1);
        Timestamp expireTime(atoi(InterceptString(data, p1 + 2, p2).c_str()));
        p1 = data.find('#', p2);
        int keyLen = atoi(InterceptString(data, p2 + 1, p1).c_str());
        std::string key = data.substr(p1 + 1, keyLen);
        p2 = data.find('!', p1);
        p1 = data.find('!', p2 + 1);
        int valueSize = atoi(InterceptString(data, p2 + 1, p1).c_str());
        int valueLen = 0;
        while (valueSize--)
        {
          p2 = data.find('#', p1);
          int valueKeyLen = atoi(InterceptString(data, p1 + 1, p2).c_str());
          std::string valueKey = data.substr(p2 + 1, valueKeyLen);
          p1 = data.find('!', p2);
          p2 = p1;
          p1 = data.find('$', p2);
          valueLen = atoi(InterceptString(data, p2 + 1, p1).c_str());
          std::string value = data.substr(p1 + 1, valueLen);
          if (valueSize > 1)
            p1 = data.find('!', p2 + 1);
          AddKey(dbobject::kDbZSet, key, valueKey, value);
        }
        p1 += 1 + valueLen;
      } while (data.substr(p1, 2) == "ST");
      continue;
    }
  }
}

bool Database::AddKey(const int type, const std::string &key,
                      const std::string &objKey, const std::string &objValue)
{
  if (type == dbobject::kDbString)
  {
    auto it = string_.find(key);
    if (it == string_.end())
    {
      string_.insert(std::make_pair(key, objKey));
    }
    else
    {
      string_[key] = objKey;
    }
  }
  else if (type == dbobject::kDbList)
  {
    auto it = list_.find(key);
    if (it == list_.end())
    {
      std::list<std::string, __gnu_cxx::__pool_alloc<std::string>> tmp;
      tmp.emplace_back(objKey);
      list_.insert(std::make_pair(key, tmp));
    }
    else
    {
      it->second.emplace_back(objKey);
    }
  }
  else if (type == dbobject::kDbHash)
  {
    auto it = hash_.find(key);
    if (it == hash_.end())
    {
      std::map<
          std::string, std::string, std::less<>,
          __gnu_cxx::__pool_alloc<std::pair<const std::string, std::string>>>
          tmp;
      tmp.insert(std::make_pair(objKey, objValue));
      hash_.insert(std::make_pair(key, tmp));
    }
    else
    {
      it->second[objKey] = objValue;
    }
  }
  else if (type == dbobject::kDbSet)
  {
    auto it = set_.find(key);
    if (it == set_.end())
    {
      std::unordered_set<std::string, std::hash<std::string>, std::equal_to<>,
                         __gnu_cxx::__pool_alloc<std::string>>
          tmp;
      tmp.insert(objKey);
      set_.insert(std::make_pair(key, tmp));
    }
    else
    {
      it->second.insert(objKey);
    }
  }
  else if (type == dbobject::kDbZSet)
  {
    auto it = zset_.find(key);
    if (it == zset_.end())
    {
      SkipListSp skipList(new Skiplist());
      skipList->InsertNode(objKey, atoi(objValue.c_str()));

      zset_.insert(std::make_pair(key, skipList));
    }
    else
    {
      auto iter = zset_.find(key);
      iter->second->InsertNode(objKey, atoi(objValue.c_str()));
    }
  }
  else
  {
    std::cout << "Unknown type" << std::endl;
    return false;
  }
  std::cout << "Add key successfully" << std::endl;
  return true;
}

bool Database::DelKey(const int type, const std::string &key)
{
  if (type == dbobject::kDbString)
  {
    auto it = string_.find(key);
    if (it != string_.end())
    {
      string_.erase(key);
      string_expire_.erase(key);
    }
    else
    {
      return false;
    }
  }
  else if (type == dbobject::kDbList)
  {
    auto it = list_.find(key);
    if (it != list_.end())
    {
      list_.erase(key);
      list_expire_.erase(key);
    }
    else
    {
      return false;
    }
  }
  else if (type == dbobject::kDbHash)
  {
    auto it = hash_.find(key);
    if (it != hash_.end())
    {
      hash_.erase(key);
      hash_expire_.erase(key);
    }
    else
    {
      return false;
    }
  }
  else if (type == dbobject::kDbSet)
  {
    auto it = set_.find(key);
    if (it != set_.end())
    {
      set_.erase(key);
      set_expire_.erase(key);
    }
    else
    {
      return false;
    }
  }
  std::cout << "Del key successfully" << std::endl;
  return true;
}

std::string Database::GetKey(const int type, const std::string &key)
{
  std::string res;

  if (!JudgeKeyExpiredTime(type, key))
  { // key没有过期
    if (type == dbobject::kDbString)
    {
      auto it = string_.find(key);
      if (it == string_.end())
      {
        res = DbStatus::NotFound("key").ToString();
      }
      else
      {
        res = it->second;
      }
    }
    else if (type == dbobject::kDbHash)
    {
      auto it = hash_.find(key);
      if (it == hash_.end())
      {
        res = DbStatus::NotFound("key").ToString();
      }
      else
      {
        std::map<std::string, std::string>::iterator iter;
        for (iter = it->second.begin(); iter != it->second.end(); iter++)
        {
          res += iter->first + ':' + iter->second + ' ';
        }
      }
    }
    else if (type == dbobject::kDbSet)
    {
      auto it = set_.find(key);
      if (it == set_.end())
      {
        res = DbStatus::NotFound("key").ToString();
      }
      else
      {
        for (const auto &iter : it->second)
        {
          res += iter + ' ';
        }
      }
    }
    else if (type ==
             dbobject::kDbZSet)
    { // ZSet中的key,可能包含range范围,格式为
      // key:low@high 或 key
      double low = -DBL_MAX;
      double high = DBL_MAX;
      std::string curKey = key;

      if (key.find(':') != std::string::npos)
      {
        int p1 = key.find(':');
        int p2 = key.find('@');
        curKey = key.substr(0, p1);
        low = std::stod(key.substr(p1 + 1, p2 - p1 - 1));
        high = std::stod(key.substr(p2 + 1, key.size() - p2));
      }
      auto it = zset_.find(curKey);
      if (it == zset_.end())
      {
        res = DbStatus::NotFound("key").ToString();
      }
      else
      {
        RangeSpec range(low, high);
        std::vector<SkiplistNode *> nodes(it->second->GetNodeInRange(range));
        for (auto node : nodes)
        {
          res += node->obj_ + ':' + std::to_string(node->score_) + '\n';
        }
        res.pop_back();
      }
    }
  }
  else
  { // key过期的情况
    // 惰性删除策略
    if (del_mode_ & dbobject::kDuoxingDel)
    {
      DelKey(type, key);
      res = "The key has expired and will be deleted";
    }
  }
  return res;
}
bool Database::SetPExpireTime(const int type, const std::string &key,
                              double expiredTime /* milliSeconds*/)
{
  if (type == dbobject::kDbString)
  {
    auto it = string_.find(key);
    if (it != string_.end())
    {
      auto now =
          addTime(Timestamp::now(), expiredTime / kMilliSecondsPerSecond);
      string_expire_[key] = now;
      // 如果过期策略采用定时删除
      if (del_mode_ & dbobject::kDingshiDel)
      {
        auto it = muduo::net::EventLoop::getEventLoopOfCurrentThread();
        it->runAt(now,
                  std::bind(&Database::DelKey, this, dbobject::kDbString, key));
      }
      return true;
    }
  }
  else if (type == dbobject::kDbList)
  {
    auto it = list_.find(key);
    if (it != list_.end())
    {
      auto now =
          addTime(Timestamp::now(), expiredTime / kMilliSecondsPerSecond);
      list_expire_[key] = now;
      // 如果过期策略采用定时删除
      if (del_mode_ & dbobject::kDingshiDel)
      {
        auto it = muduo::net::EventLoop::getEventLoopOfCurrentThread();
        it->runAt(now,
                  std::bind(&Database::DelKey, this, dbobject::kDbList, key));
      }
      return true;
    }
  }
  else if (type == dbobject::kDbHash)
  {
    auto it = hash_.find(key);
    if (it != hash_.end())
    {
      auto now =
          addTime(Timestamp::now(), expiredTime / kMilliSecondsPerSecond);
      hash_expire_[key] = now;
      // 如果过期策略采用定时删除
      if (del_mode_ & dbobject::kDingshiDel)
      {
        auto it = muduo::net::EventLoop::getEventLoopOfCurrentThread();
        it->runAt(now,
                  std::bind(&Database::DelKey, this, dbobject::kDbHash, key));
      }
      return true;
    }
  }
  else if (type == dbobject::kDbSet)
  {
    auto it = set_.find(key);
    if (it != set_.end())
    {
      auto now =
          addTime(Timestamp::now(), expiredTime / kMilliSecondsPerSecond);
      set_expire_[key] = now;
      // 如果过期策略采用定时删除
      if (del_mode_ & dbobject::kDingshiDel)
      {
        auto it = muduo::net::EventLoop::getEventLoopOfCurrentThread();
        it->runAt(now,
                  std::bind(&Database::DelKey, this, dbobject::kDbSet, key));
      }
      return true;
    }
  }
  else if (type == dbobject::kDbZSet)
  {
    auto it = zset_.find(key);
    if (it != zset_.end())
    {
      auto now =
          addTime(Timestamp::now(), expiredTime / kMilliSecondsPerSecond);
      zset_expire_[key] = now;
      // 如果过期策略采用定时删除
      if (del_mode_ & dbobject::kDingshiDel)
      {
        auto it = muduo::net::EventLoop::getEventLoopOfCurrentThread();
        it->runAt(now,
                  std::bind(&Database::DelKey, this, dbobject::kDbZSet, key));
      }
      return true;
    }
  }
  return false;
}

bool Database::SetPExpireTime(const int type, const std::string &key,
                              const Timestamp &expiredTime)
{
  double expired_time =
      expiredTime.microSecondsSinceEpoch() / kMicroSecondsPerMilliSecond;
  return SetPExpireTime(type, key, expired_time);
}

Timestamp Database::GetKeyExpiredTime(const int type, const std::string &key)
{
  Timestamp tmp;
  if (type == dbobject::kDbString)
  {
    auto it = string_expire_.find(key);
    if (string_expire_.find(key) != string_expire_.end())
    {
      tmp = it->second;
    }
    else
    {
      tmp = Timestamp::invalid();
    }
  }
  else if (type == dbobject::kDbList)
  {
    auto it = list_expire_.find(key);
    if (it != list_expire_.end())
    {
      tmp = it->second;
    }
    else
    {
      tmp = Timestamp::invalid();
    }
  }
  else if (type == dbobject::kDbHash)
  {
    auto it = hash_expire_.find(key);
    if (it != hash_expire_.end())
    {
      tmp = it->second;
    }
    else
    {
      tmp = Timestamp::invalid();
    }
  }
  else if (type == dbobject::kDbSet)
  {
    auto it = set_expire_.find(key);
    if (it != set_expire_.end())
    {
      tmp = it->second;
    }
    else
    {
      tmp = Timestamp::invalid();
    }
  }
  else if (type == dbobject::kDbZSet)
  {
    auto it = zset_expire_.find(key);
    if (it != zset_expire_.end())
    {
      tmp = it->second;
    }
    else
    {
      tmp = Timestamp::invalid();
    }
  }

  return tmp;
}

bool Database::JudgeKeyExpiredTime(const int type, const std::string &key)
{
  Timestamp expired = GetKeyExpiredTime(type, key);
  if (expired == Timestamp::invalid())
  {
    return false;
  }

  Timestamp now = Timestamp::now();
  return now > expired;
}

const std::string Database::RPopList(const std::string &key)
{
  auto iter = list_.find(key);
  if (iter != list_.end())
  {
    if (iter->second.empty())
    {
      return DbStatus::NotFound("nil").ToString();
    }
    std::string res = iter->second.back();
    iter->second.pop_back();
    return res;
  }
  else
  {
    return DbStatus::NotFound("key").ToString();
  }
}

std::string Database::InterceptString(const std::string &ss, int p1, int p2)
{
  return ss.substr(std::min(p1, p2), abs(p2 - p1));
}