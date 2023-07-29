/**
 * @file db_status.h
 * @author pengchang

 */
#ifndef DB_STATUS_H
#define DB_STATUS_H
#include <string>

// 数据库命令操作响应
class DbStatus
{
public:
  ~DbStatus() = default;

  std::string ToString()
  {
    if (msg_ == "")
    {
      return "OK\n";
    }
    else
    {
      std::string type;
      switch (db_state_)
      {
      case kOK:
        type = "OK: ";
        break;
      case kNotFound:
        type = "NotFound: ";
        break;
      case kIOError:
        type = "IO Error: ";
        break;
      default:
        break;
      }
      std::string res = type + msg_ + "\n";
      return res;
    }
  }
  /**
   * @brief 构造DbStatus对象
   * @details 只能通过静态方法创建DbStatus对象，
   * 使用示例：DbStatus::IOError("Parameter error")
   * @return DbStatus
   */
  static DbStatus Ok() { return DbStatus(); }
  static DbStatus NotFound(const std::string &msg)
  {
    return DbStatus(kNotFound, msg);
  }
  static DbStatus IOError(const std::string &msg)
  {
    return DbStatus(kIOError, msg);
  }

private:
  DbStatus() : db_state_(ResCode::kOK), msg_("") {}
  DbStatus(int dbState, const std::string &msg)
      : db_state_(dbState), msg_(msg) {}

private:
  // 操作结果代码
  enum ResCode
  {
    kOK = 0,
    kNotFound,
    kIOError
  };
  int db_state_;
  std::string msg_;
};
#endif