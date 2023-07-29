/**
 * @file database.h
 * @author pengchang
 * @brief

 */
#ifndef DATABASE_H
#define DATABASE_H

#include <muduo/base/Timestamp.h>
#include <signal.h>
#include <sys/time.h>

#include <ext/pool_allocator.h>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "db_obj.h"
#include "skiplist.h"
using SkipListSp = Skiplist::ptr;
using Timestamp = muduo::Timestamp;

// 使用STL中unordered_map作为字典
template <typename T1, typename T2>
using Dict =
    std::unordered_map<T1, T2, std::hash<T1>, std::equal_to<T1>,
                       __gnu_cxx::__pool_alloc<std::pair<const T1, T2>>>;

// 数据库键类型为std::string
// 数据库五种值类型定义
// __pool_alloc内部使用链表进行管理内存、预分配内存、不归还内存给操作系统等机制来减少malloc的调用次数。
using String = Dict<std::string, std::string>;
using List = Dict<std::string,
                  std::list<std::string, __gnu_cxx::__pool_alloc<std::string>>>;
using Hash =
    Dict<std::string, std::map<std::string, std::string, std::less<>,
                               __gnu_cxx::__pool_alloc<
                                   std::pair<const std::string, std::string>>>>;
using Set =
    Dict<std::string, std::unordered_set<std::string, std::hash<std::string>,
                                         std::equal_to<>,
                                         __gnu_cxx::__pool_alloc<std::string>>>;
using ZSet = Dict<std::string, SkipListSp>;
using Expire = Dict<std::string, Timestamp>;

class Database
{
public:
  Database();
  ~Database() = default;
  /**
   * @brief 从rdb文件中恢复数据库
   * @param[in] index 数据库分库编号
   */
  void RdbLoad(int index);
  bool AddKey(const int type, const std::string &key, const std::string &objKey,
              const std::string &objValue);
  bool DelKey(const int type, const std::string &key);
  std::string GetKey(const int type, const std::string &key);
  bool SetPExpireTime(const int type, const std::string &key,
                      double expiredTime);
  bool SetPExpireTime(const int type, const std::string &key,
                      const Timestamp &expiredTime);

  /**
   * @brief 求过期时间
   */
  Timestamp GetKeyExpiredTime(const int type, const std::string &key);

  /**
   * @brief 判断是否过期
   */
  bool JudgeKeyExpiredTime(const int type, const std::string &key);
  const std::string RPopList(const std::string &key);

  String &GetKeyStringObj() { return string_; }
  List &GetKeyListObj() { return list_; }
  Hash &GetKeyHashObj() { return hash_; }
  Set &GetKeySetObj() { return set_; }
  ZSet &GetKeyZSetObj() { return zset_; }
  // 得到当前数据库键的数目
  int GetKeySize() const
  {
    return GetKeyStringSize() + GetKeyListSize() + GetKeyHashSize() +
           GetKeySetSize() + GetKeyZSetSize();
  }
  int GetKeyStringSize() const { return string_.size(); };
  int GetKeyListSize() const { return list_.size(); }
  int GetKeyHashSize() const { return hash_.size(); }
  int GetKeySetSize() const { return set_.size(); }
  int GetKeyZSetSize() const { return zset_.size(); }

private:
  std::string InterceptString(const std::string &ss, int p1, int p2);
  // void DingshiHandler(const std::string &key);

  /**
   * @brief 定期删除
   */
  void DingqiHandler();

private:
  // 过期删除策略
  // 惰性删除只需要在查该key时判断一下过期没有即可，过期则删除
  short del_mode_ = dbobject::kDuoxingDel | dbobject::kDingqiDel;

  // 五个数据字典
  String string_;
  List list_;
  Hash hash_;
  Set set_;
  ZSet zset_;

  // 五个过期字典
  Expire string_expire_;
  Expire list_expire_;
  Expire hash_expire_;
  Expire set_expire_;
  Expire zset_expire_;
};
#endif