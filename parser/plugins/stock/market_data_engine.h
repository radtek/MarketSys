//  Copyright (c) 2015-2015 The KID Authors. All rights reserved.
//  Created on: 2016/10/13 Author: zjc

#ifndef SRC_PLUGINS_STOCK_MARKET_DATA_ENGINE_H_
#define SRC_PLUGINS_STOCK_MARKET_DATA_ENGINE_H_

#include <string>

#include "mem_pool.h"
#include "realtime_code_info.h"
#include "market_time_manager.h"

namespace base_logic {
class RedisController;
}
using base_logic::RedisController;

namespace stock_logic {

class DataEngine {
 public:
  typedef std::map<std::string, RealtimeCodeInfo*> CodeMap;
  typedef std::map<int, CodeMap> DataMap;
  typedef std::vector<std::string> StringArray;
  typedef std::vector<StringArray> StringArray2;
  typedef std::vector<int> TimeList;
  typedef std::vector<RealtimeCodeInfo> CodeInfoArray;
  enum Type {
    SH,
    SZ,
    MAX_TYPE
  };

  enum DbTable {
    TODAY_STOCK,
    TODAY_STOCK_ALL_INFO
  };

  struct Option {
    Type type;
    std::string hkey_prefix;
    StockDB* db;
    RedisController* redis;
  };

 public:
  void Init(Type type, const std::string& hkey_prefix,
            StockDB* db, RedisController* redis);
  DataEngine();
  ~DataEngine();
  void OnTime();
  int market_time() const { return current_->mtime_set.market_time(); }
  bool UpdateRealtimeData(int min_time);

  bool is_work_time() const {
    time_t now = time(NULL);
    struct tm* pt = localtime(&now);
    int t = pt->tm_hour * 100 + pt->tm_min;
    if (t >= START_WORK_TIME && t <= END_WORK_TIME) {
      return true;
    }
    return false;
  }

 private:
  void switch_to_prev_buffer() {
    if (--current_ < &data_[0]) {
      current_ = &data_[BUFFER_DAYS-1];
    }
  }

  void switch_to_next_buffer() {
    ++current_;
    if (current_ > &data_[BUFFER_DAYS-1]) {
      current_ = &data_[0];
    }
  }

  bool FillBuffer(const std::string& trade_date);

  bool Update();
  bool FilterNewData(const StringArray& all_times, StringArray& new_times);
  bool Append(int market_time, const CodeInfoArray& data);
  bool UpdateDb(int mtime, DbTable table);

  void Reset();

  bool FetchRawData(const std::string& date,
                 const std::string& market_time, CodeInfoArray& res);
  bool Parse(const std::string& info, StringArray2& res);
  bool ParseLine(const std::string& line, StringArray& res);

  void ProcessIndex(RealtimeCodeInfo& info);
 private:
  const static char kShIndex[];           // 上证指数
  const static char kShSz300[];           // 沪深300
  const static char kSzComponentIndex[];  // 深证成指
  const static char kGEMIndex[];          // 创业板指

  const static int CODE_COUNT = 2000;
  const static int BUFFER_DAYS = 1;
  const static int MEM_POOL_SIZE = CODE_COUNT * sizeof(RealtimeCodeInfo);
  const static int START_WORK_TIME = 915;
  const static int END_WORK_TIME = 1530;

  struct Data {
    std::string trade_date;
    MemPool mem_pool;
    DataMap data_map;
    MarketTimeManager mtime_set;

    Data()
        : mem_pool(MEM_POOL_SIZE) {}

    void reset() {
      // 清内存池之前, 需要先调用析构函数,由于析构函数是空的,因此此处省略
      mem_pool.Reset();
      data_map.clear();
      mtime_set.reset();
    }
  };
 private:
  Type type_;
  std::string hkey_prefix_;

  bool initialized_;
  StockDB* db_;
  RedisController* redis_;

  threadrw_t *lock_;

  Data data_[BUFFER_DAYS];
  Data* current_;
};

} /* namespace stock_logic */

#endif /* SRC_PLUGINS_STOCK_MARKET_DATA_ENGINE_H_ */
