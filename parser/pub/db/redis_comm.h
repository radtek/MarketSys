// Copyright (c) 2015-2015 The prism Authors. All rights reserved.
// Created on: 2015/11/25 Author: jiaoyongqing
#ifndef _HOME_RUNNER_NORTHSEA_RESTFUL_PUB_DB_REDIS_COMM_H_
#define _HOME_RUNNER_NORTHSEA_RESTFUL_PUB_DB_REDIS_COMM_H_

#include <list>
#include <map>
#include <string>

#include "logic/logic_basic_info.h"
#include "config/config.h"
#include "storage/storage.h"
#include "basic/basic_info.h"
#include "net/typedef.h"

namespace db {

class DbRedis {
 public:
  DbRedis();
  ~DbRedis();

 public:
  static void Init(std::list<base::ConnAddr> *const addrlist);
  static void Dest();

 public:
  base_storage::DictionaryStorageEngine* GetRedis();

  //  默认表示得到所有股票在某一个时段的visti | search | attent 的总数。
  //  非默认，得到某一支股票在某一个时段的visit | search | attent 的总数。
  int64 GetTotal(int rtype, time_t start, time_t end, \
                   std::string stock_code, int province);

  //  默认表示得到所有股票在某一个时间点的visti | search | attent 的总数。
  //  非默认，得到某一支股票在某一个时间点的visit | search | attent 的总数。
  int64 GetVsaNum(std::string rtype,  time_t t, \
                 std::string stock_code, int province);

  //  得到所有股票在某一个时段的visti | search | attent 的总数。
  void GetAll(int rtype, time_t start, time_t end, MapStrToStr *const ret, int province);

  void GetStockFrequencyBetweenCurrentDay(int rtype, \
                                   const char *head, \
                                   const char *tail, \
                                          int order, \
                                 MapStrToStr *map_ret, \
                                 int province);

  void GetGSKZBetweenCurrentDay( const char *head, \
                                 const char *tail, \
                                        int order, \
                              MapStrToStr *map_ret);

  int64 GetTotalFrequencyBetweenCurrentDay(int rtype, int province);

  std::string GetStockUpDown(const std::string &code);

  int get_error_info() { return error_info_; }

 public:
  int error_info_;
  base_storage::DictionaryStorageEngine* redis_;
};

}  // namespace db

#endif  // _HOME_RUNNER_NORTHSEA_RESTFUL_PUB_DB_REDIS_COMM_H_
