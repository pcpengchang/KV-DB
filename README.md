该项目主要使用C++实现了一个键值型数据库，它的功能包括对键值对的增加、删除、修改、查询、设置过期时间等操作，并使用第三方网络库发布提供对外存储服务。

1. 实现基础结构skiplist，使用stl中unordered_map作为字典，实现字符串、列表、哈希、集合、有序集合五种值类型。
2. 使用muduo网络库，对外提供数据存储服务，并支持get、set、expire等常用命令。
3. 实现了定时删除、定期删除和惰性删除的过期删除策略。
4. 对数据库实现了简易的rdb数据存盘机制。

## 使用

依赖于muduo网络库。

默认服务端地址: 0.0.0.0:10000(可在store_server.cc中修改)

```shell
chmod +x build.sh
./build.sh
./bin/store_server
```

## 架构

项目架构图：

![image-20220913091526801](https://lei-typora-image.oss-cn-chengdu.aliyuncs.com/image-20220913091526801.png)

类型定义：

```cpp
using SkipListSp = Skiplist::ptr;
using Timestamp = muduo::Timestamp;
// 使用STL中unordered_map作为字典
template <typename T1, typename T2>
using Dict =
    std::unordered_map<T1, T2, std::hash<T1>, std::equal_to<T1>,
                       __gnu_cxx::__pool_alloc<std::pair<const T1, T2>>>;
// 数据库键类型为std::string。
// 数据库五种值类型定义
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
```



## TODO

- 实现AOF机制。
- 支持更多命令。
- 实现内存淘汰策略。
- 完成配置模块，实现从文件中拉取数据库配置项。
- 添加分布式相关，raft算法。