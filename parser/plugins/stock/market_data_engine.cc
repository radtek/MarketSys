//  Copyright (c) 2015-2015 The KID Authors. All rights reserved.
//  Created on: 2016/10/13 Author: zjc

#include "market_data_engine.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include <sstream>

#include "logic/logic_comm.h"
#include "stock_util.h"
#include "storage/redis_controller.h"

#ifndef speedtest__
#define speedtest__(data)   for (long blockTime = NULL; \
  (blockTime == NULL ? (blockTime = clock()) != NULL : false); \
  LOG_MSG2(data "%.6lfs", (double) (clock() - blockTime) / CLOCKS_PER_SEC))
#endif

namespace stock_logic {

//static FILE* hFile = NULL;
//static const char filename[] = "err_file.txt";

const char DataEngine::kShIndex[] = "000001";
const char DataEngine::kShSz300[] = "000300";
const char DataEngine::kSzComponentIndex[] = "399001";
const char DataEngine::kGEMIndex[] = "399006";

DataEngine::DataEngine()
    : type_(SH),
      lock_(NULL),
      db_(NULL),
      redis_(NULL),
      initialized_(false),
      current_(&data_[0]) {
  InitThreadrw(&lock_);
}

DataEngine::~DataEngine() {
}

void DataEngine::Init(Type type, const std::string& hkey_prefix,
          StockDB* db, RedisController* redis) {
  type_ = type;
  hkey_prefix_ = hkey_prefix;
  db_ = db;
  redis_ = redis;

  char str_date[10];
  int buf_days = BUFFER_DAYS;
  StockUtil* util = StockUtil::Instance();
  int date = util->Date();
  if (util->is_trading_day()) --buf_days;
  for (size_t i = 0; i < buf_days; ++i) {
    date = util->PreTradeDate(date);
    sprintf(str_date, "%d", date);
    current_->trade_date = str_date;
    LOG_MSG2("init %d data", date);
    if (!FillBuffer(str_date)) {
      LOG_ERROR2("init %s data", date);
    }
    switch_to_prev_buffer();
  }
  switch_to_prev_buffer();

  LOG_MSG2("RealtimeCodeInfo size: %d, mem_pool_size: %d",
           sizeof(RealtimeCodeInfo), MEM_POOL_SIZE);
}

bool DataEngine::Parse(const std::string& info, StringArray2& res) {
  if (info.empty()) {
    return false;
  }
  std::stringstream ss(info);
  std::string line;
  while (getline(ss, line, ';')) {
    StringArray v;
//    LOG_DEBUG2("line: %s", line.c_str());
    ParseLine(line, v);
    res.push_back(v);
  }
  return true;
}

bool DataEngine::ParseLine(const std::string& line, StringArray& res) {
  std::stringstream ss(line);
  std::string field;
  RealtimeCodeInfo code;
  res.clear();
  while (getline(ss, field, ',')) {
//    LOG_DEBUG2("field: %s", field.c_str());
    res.push_back(field);
  }
  return true;
}

bool DataEngine::FilterNewData(const StringArray& all_times,
                               StringArray& new_times) {
  new_times.clear();
  for (size_t i = 0; i < all_times.size(); ++i) {
    if (all_times[i].empty()) {
      continue;
    }

    int t = atoi(all_times[i].c_str());
//    if (!current_->mtime_set.exist(t)) {
    if (0 == current_->data_map.count(t)) {
      new_times.push_back(all_times[i]);
    }
  }
  return !new_times.empty();
}

void DataEngine::ProcessIndex(RealtimeCodeInfo& info) {
  if (SH == type_) {
    if (kShIndex == info.code) {
      info.code = "sh" + info.code;
    } else if (kShSz300 == info.code) {
      info.code = "hs300";
    }
  } else if (SZ == type_) {
    if (kSzComponentIndex == info.code
        || kGEMIndex == info.code) {
      info.code = "sh" + info.code;
    }
  }
}

bool DataEngine::FetchRawData(const std::string& date,
                                     const std::string& market_time,
                              CodeInfoArray& res) {
  std::string raw_data;
  std::string hkey = hkey_prefix_ + date;
  if (!redis_->GetHashElement(hkey, market_time, raw_data)) {
    LOG_ERROR2("fetch data error, hkeys: %s, market_time: %s",
        hkey.c_str(), market_time.c_str());
    return false;
  }

  StringArray2 fields;
  Parse(raw_data, fields);

  res.clear();
  res.reserve(fields.size());

//  static bool init = false;
//  if (!init) {
//    init = true;
//    hFile = fopen(filename, "a+");
//    if (NULL == hFile) {
//      LOG_ERROR2("open file error, name: %s, err: %",
//                 filename, strerror(errno));
//    }
//  }
//
//  std::string str = "-------------------------------" + market_time + "\n";
//  fwrite(str.c_str(), 1, str.size(), hFile);

  for (size_t i = 0; i < fields.size(); ++i) {
    RealtimeCodeInfo code;
    if (code.Deserialize(fields[i])) {
      ProcessIndex(code);
      res.push_back(code);
    }
//    else {
//      if (!code.Delist()) {
//        std::string msg = code.Serialized();
//        msg.push_back('\n');
//        fwrite(msg.c_str(), 1, msg.size(), hFile);
//      }
//    }
  }
  return !res.empty();
}

bool DataEngine::FillBuffer(const std::string& trade_date) {
  StringArray time_list;
  std::string hkey = hkey_prefix_ + trade_date;
  if (!redis_->GetAllHashFields(hkey, time_list)) {
    LOG_ERROR2("get time list error: %s", hkey.c_str());
    return false;
  }

  CodeInfoArray data;
  for (size_t i = 0; i < time_list.size(); ++i) {
    if (!FetchRawData(trade_date, time_list[i], data)
        || data.empty()) {
      continue;
    }

    LOG_MSG2("build time: %s", time_list[i].c_str());
    int t = atoi(time_list[i].c_str());
    current_->mtime_set.Add(t);

    CodeMap& code_map = current_->data_map[t];
    for (size_t j = 0; j < data.size(); ++j) {
//      LOG_DEBUG2("construct code: %s", data[j].code.c_str());
      RealtimeCodeInfo* code = current_->mem_pool.Construct(data[j]);
//      RealtimeCodeInfo* code = new RealtimeCodeInfo(data[i]);
      code_map.insert(CodeMap::value_type(code->code, code));
    }
  }
  return true;
}

bool DataEngine::Append(int market_time, const CodeInfoArray& data) {
  CodeMap& code_map = current_->data_map[market_time];
  LOG_DEBUG2("time: %d, data size: %d", market_time, data.size());
  for (size_t i = 0; i < data.size(); ++i) {
    RealtimeCodeInfo* code = current_->mem_pool.Construct(data[i]);
//    RealtimeCodeInfo* code = new RealtimeCodeInfo(data[i]);
    code_map.insert(CodeMap::value_type(code->code, code));
  }
  return true;
}

bool DataEngine::Update() {
  StringArray all_times;
  std::string hkey = hkey_prefix_ + current_->trade_date;
  if (!redis_->GetAllHashFields(hkey, all_times)) {
    LOG_ERROR2("get market time error: %s", hkey.c_str());
    return false;
  }

  StringArray new_times;
  if (!FilterNewData(all_times, new_times)) {
    return false;
  }

  CodeInfoArray data;
  for (size_t i = 0; i < new_times.size(); ++i) {
    if (!FetchRawData(current_->trade_date, new_times[i], data)
        || data.empty()) {
      continue;
    }
    int t = atoi(new_times[i].c_str());
    current_->mtime_set.Add(t);
    Append(t, data);
  }

  int new_mtime = -1;
  int newest_mtime = -1;
  while (1) {
    new_mtime = current_->mtime_set.NewMarketTime();
    if (-1 == new_mtime) {
      break;
    }

    newest_mtime = new_mtime;
    UpdateDb(new_mtime, TODAY_STOCK_ALL_INFO);
  }
  return db_->DelPreTradeData();
}

bool DataEngine::UpdateRealtimeData(int min_time) {
  int nearest_time = current_->mtime_set.NearestMarketTime(min_time);
  if (-1 == nearest_time) {
    LOG_ERROR2("not exist time: %d", min_time);
    return false;
  }
  if (!UpdateDb(nearest_time, TODAY_STOCK)) {
    LOG_ERROR2("update realtime data error, mtime: %d", min_time);
    return false;
  }
//  db_->DelOldRealtimeData(StockUtil::Instance()->to_timestamp(min_time));
  return true;
}

bool DataEngine::UpdateDb(int mtime, DbTable table) {
  DataMap& tmap = current_->data_map;
  DataMap::iterator it = tmap.find(mtime);
  if (tmap.end() == it) {
    LOG_ERROR2("not exist mtime: %d", mtime);
    return false;
  }

  StockUtil* util = StockUtil::Instance();
  int market_time = current_->mtime_set.market_time();
  int ts = util->to_timestamp(market_time);
  CodeMap& cmap = it->second;
  StockDB::CodeInfoArray data;
  static const int num = 500;
  data.reserve(num);

  bool (StockDB::*Callback)(const StockDB::CodeInfoArray&) =
      &StockDB::UpdateTodayStockAllInfo;
  if (TODAY_STOCK == table) {
    Callback = &StockDB::UpdateTodayStock;
  }

  for (CodeMap::iterator it = cmap.begin(); cmap.end() != it; ++it) {
    StockDB::CodeRealtimeInfo info = it->second->CreateDbCodeInfo();
    info.market_time = ts;
    data.push_back(info);
    if (data.size() == num) {
        if (!(db_->*Callback)(data)) {
          LOG_ERROR2("update error, table: %d, code num: %d",
              table, data.size());
        }
      data.clear();
    }
  }
  if (!(db_->*Callback)(data)) {
    LOG_ERROR2("update error, table: %d, code num: %d",
        table, data.size());
  }
  return true;
}

void DataEngine::OnTime() {
  if (!(StockUtil::instance_->is_trading_day()
      && is_work_time())) {
    initialized_ = false;
    return ;
  }

  if (!initialized_) {
    initialized_ = true;
//    db_->ClearTodayStockAllInfo();

    char str_date[10];
    switch_to_next_buffer();
    current_->reset();
    StockUtil* util = StockUtil::Instance();
    int today = util->Date();
    sprintf(str_date, "%d", today);
    current_->trade_date = str_date;
    LOG_MSG2("new trade day: %d", today);
//    FillBuffer(buf);
  }
  Update();
}

} /* namespace stock_logic */
