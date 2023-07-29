/**
 * @file skiplist.h
 * @author pengchang
 * @brief 实现一个跳表，用于实现Zset值类型

 */

#ifndef SKIPLIST_H
#define SKIPLIST_H
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#define MAX_LEVEL 12

class SkiplistLevel;

// class for Node
class SkiplistNode
{
public:
  SkiplistNode() = default;
  SkiplistNode(const std::string &obj, double score, int level);

  std::unique_ptr<std::unique_ptr<SkiplistLevel>[]> levels_;
  std::string obj_;
  double score_;
};

// class for level
class SkiplistLevel
{
public:
  SkiplistLevel() : forward_(nullptr) {}
  SkiplistNode *forward_;
};

// class for range
struct RangeSpec
{
  RangeSpec(double min, double max)
      : min_(min), max_(max), minex_(true), maxex_(true) {}

  double min_, max_;
  bool minex_, maxex_;
};

// class for skip list
class Skiplist
{
public:
  using ptr = std::shared_ptr<Skiplist>;
  Skiplist();
  ~Skiplist();
  Skiplist(Skiplist &) = delete;
  Skiplist &operator=(Skiplist &) = delete;

  SkiplistNode *CreateNode(const std::string &obj, double score, int level);
  int GetRandomLevel();
  void InsertNode(const std::string &, double);
  void DeleteNode(const std::string &, double);
  unsigned long GetCountInRange(RangeSpec &range);
  std::vector<SkiplistNode *> GetNodeInRange(RangeSpec &range);
  unsigned long GetLength() { return length_; }

private:
  int ValueGteMin(double value, RangeSpec &spec);
  int ValueLteMax(double value, RangeSpec &spec);

private:
  // 头节点
  SkiplistNode *header_;

  // 用来保证key不同
  std::unordered_map<std::string, double> key_set_;

  int level_;
  unsigned long length_;
};
#endif