/**
 * @file db_obj.h
 * @author pengchang

 */
#ifndef DB_OBJ_H
#define DB_OBJ_H
#include <muduo/base/Timestamp.h>

/**
 * @brief 存放与数据库有关常量
 */
namespace dbobject
{
    // 数据类型
    const short kDbString = 0;
    const short kDbList = 1;
    const short kDbHash = 2;
    const short kDbSet = 3;
    const short kDbZSet = 4;

    const std::string kDefaultObjValue = "NULL";

    // RDB默认保存时间(ms)
    const muduo::Timestamp kRdbDefaultTime(1000 * 1000 * 1000);

    // 过期删除策略
    const short kDingshiDel = 0x0;
    const short kDuoxingDel = 0x1;
    const short kDingqiDel = 0x2;

    // 定期删除每次抽查的key数量
    const int kCheckNum = 20;
} // namespace dbobject
#endif