// Copyright (c) 2015-2015 The restful Authors. All rights reserved.
// Created on: 2015/11/24 Author: jiaoyongqing

#include "db/redis_comm.h"

#include <mysql.h>
#include <unistd.h>

#include <string>
#include <sstream>

#include "tools/tools.h"
#include "logic/logic_comm.h"
#include "dic/base_dic_redis_auto.h"
#include "basic/template.h"
#include "db/base_db_mysql_auto.h"
#include "net/error_comm.h"

namespace db {

DbRedis::DbRedis() {
  redis_ = base_dic::RedisPool::RedisConnectionPop();
}

DbRedis::~DbRedis() {
  base_dic::RedisPool::RedisConnectionPush(redis_);
}

void DbRedis::Dest() {
  base_dic::RedisPool::Dest();
}

void DbRedis::Init(std::list<base::ConnAddr> * const addrlist) {
  base_dic::RedisPool::Init(*addrlist);
}

base_storage::DictionaryStorageEngine* DbRedis::GetRedis() {
  int num = 0;
  error_info_ = 0;
  while(num < 5) {
    if (redis_ != NULL) {
      break;
    }
    usleep(1000);
    redis_ = base_dic::RedisPool::RedisConnectionPop();
    ++num;
  }

  if (redis_ == NULL) {
    LOG_ERROR("Redis GetConnection Error");
    error_info_ = CONNECT_ERROR;
    return NULL;
  }

  return redis_;
}

int64 DbRedis::GetTotal(int rtype, \
                     time_t start, \
                       time_t end, \
           std::string stock_code, \
           int province) {
  start = start - (start % 3600);
  std::string diff = (stock_code == std::string("")) ? "" : stock_code;
  std::string key;
  int key_len = 0;
  char *val = NULL;
  size_t val_len = 0;
  int64 total = 0;
  for (int64 i = start; i <= end; i += 3600) {
    switch (rtype) {
      case VISIT:
        total += GetVsaNum(REDIS_VISIT, i,  diff, province);
        break;
      case SEARCH:
        total += GetVsaNum(REDIS_SEARCH, i,  diff, province);
        break;
      case FOLLOW:
        total += GetVsaNum(REDIS_FOLLOW, i,  diff, province);
        break;
    }
  }

  return total;
}

int64 DbRedis::GetVsaNum(std::string rtype, time_t t, std::string stock_code, int province) {
  base_storage::DictionaryStorageEngine* redis = GetRedis();
  if (NULL == redis) {
    error_info_ = CONNECT_ERROR;
    return 0;
  }

  std::string diff;
  if (stock_code == std::string("")) {
    diff = std::string(REDIS_COUNT);
  } else {
    diff = stock_code;
  }
  
  std::string str_province = tools::GetProvinceString(province);
  std::string key = str_province + rtype + ":" + diff + ":" + tools::GetTimeKey(t);
  size_t key_len = key.length();
  char *val = NULL;
  size_t val_len = 0;
  bool r = redis->GetValue(key.c_str(), key_len, &val, &val_len);
  time_t current_time = time(NULL);
  error_info_ = 0;
  if (false == r && (current_time - t > 3600 * 2)) {
    LOG_ERROR("exec redis error");
    error_info_ = EXEC_REDIS_ERROR;
  }

  return (val == NULL) ? 0 : atoll(val);
}

void DbRedis::GetAll(int rtype, time_t start, time_t end, MapStrToStr *ret, int province) {
  base_storage::DictionaryStorageEngine* redis = GetRedis();
  if (NULL == redis) {
    error_info_ = CONNECT_ERROR;
    return;
  }

  std::string type;
  switch (rtype) {
    case VISIT:
      type = REDIS_VISIT;
      break;
    case SEARCH:
      type = REDIS_SEARCH;
      break;
    case FOLLOW:
      type = REDIS_FOLLOW;
      break;
  }

  std::string str_province = tools::GetProvinceString(province);

  bool r = false;
  error_info_ = 0;
  time_t current_time = time(NULL);
  std::stringstream os;
  std::string key;
  int key_len;
  MapStrToStr map_tmp;
  start = start - (start % 3600);
  for (int i = start; i <= end; i += 3600) {
    key = str_province + type + std::string(":") + tools::GetTimeKey(i);
    key_len = key.length();
    map_tmp.clear();
    r = redis->GetAllHash(key.c_str() , key_len, map_tmp);
    if (false == r && (current_time - i > 3600 * 2)) {
      LOG_ERROR("exec redis error");
      error_info_ = EXEC_REDIS_ERROR;
      return;
    }
    for (MapStrToStr::iterator it = map_tmp.begin(); \
                          it != map_tmp.end(); ++it) {
      MapStrToStr::iterator temp_it = ret->find(it->first);
      if (temp_it == ret->end()) {
        (*ret)[it->first] = it->second;
      } else {
        int64 num = atoll(temp_it->second.c_str()) +\
                             atoll(it->second.c_str());
        os.str("");
        os << num;
        (*ret)[temp_it->first] = os.str();
      }
    }
  }
}

void DbRedis::GetGSKZBetweenCurrentDay( const char *head, \
                               const char *tail, \
                                      int order, \
                           MapStrToStr *map_ret) {
  base_storage::DictionaryStorageEngine* redis = GetRedis();
  if (NULL == redis) {
    error_info_ = CONNECT_ERROR;
    return;
  }

  char buf[64];
  char *val = NULL;
  size_t val_len = 0;
  time_t now = time(NULL);
  strftime(buf,sizeof(buf),"newstock:%Y-%m-%d",localtime(&now));
  bool r = redis->GetSortedSet(buf, \
                                19, \
                              head, \
                              tail, \
                             order, \
                            *map_ret);

  error_info_ = 0;
  if (false == r) {
    LOG_ERROR("exec redis error");
    error_info_ = EXEC_REDIS_ERROR;
    return;
  }

  return;
}

void DbRedis::GetStockFrequencyBetweenCurrentDay(int rtype, \
                                          const char *head, \
                                          const char *tail, \
                                                 int order, \
                                      std::map<std::string, \
                                     std::string> *map_ret, \
                                     int province) {
  base_storage::DictionaryStorageEngine* redis = GetRedis();
  if (NULL == redis) {
    error_info_ = CONNECT_ERROR;
    return;
  }

  std::string type;
  switch (rtype) {
    case VISIT:
      type = REDIS_VISIT + std::string(":");
      break;

    case SEARCH:
      type = REDIS_SEARCH + std::string(":");
      break;

    case FOLLOW:
      type = REDIS_FOLLOW + std::string(":");
      break;

    default: break;
  }

  std::string str_province = tools::GetProvinceString(province);

  std::string current_time = tools::GetTimeKey(time(NULL));
  current_time = current_time.substr(0, 10);
  std::string key = str_province + std::string("set:") + type + current_time;
  int key_len = key.length();
  bool r = redis->GetSortedSet(key.c_str(), \
                                   key_len, \
                                      head, \
                                      tail, \
                                     order, \
                                    *map_ret);
  error_info_ = 0;
  if (false == r) {
    LOG_ERROR("exec redis error");
    error_info_ = EXEC_REDIS_ERROR;
    return;
  }

  return;
}

int64 DbRedis::GetTotalFrequencyBetweenCurrentDay(int rtype, int province) {
  base_storage::DictionaryStorageEngine* redis = GetRedis();
  if (NULL == redis) {
    error_info_ = CONNECT_ERROR;
    return 0;
  }

  std::string type;
  switch (rtype) {
    case VISIT:
      type = std::string(REDIS_VISIT) + ":" + REDIS_COUNT + std::string(":");
      break;

    case SEARCH:
      type = std::string(REDIS_VISIT) + ":" + REDIS_COUNT + std::string(":");
      break;

    case FOLLOW:
      type = std::string(REDIS_FOLLOW) + ":" + REDIS_COUNT + std::string(":");
      break;

    default: break;
  }

  std::string str_province = tools::GetProvinceString(province);

  std::string current_time = tools::GetTimeKey(time(NULL));
  current_time = current_time.substr(0, 10);
  std::string key = str_province + std::string("set:") + type + current_time;
  LOG_DEBUG2("key_count %s", key.c_str());
  size_t key_len = key.length();
  char *val = NULL;
  size_t val_len = 0;
  bool r = redis->GetValue(key.c_str(), key_len, &val, &val_len);
  error_info_ = 0;
  if (false == r) {
    LOG_ERROR("exec redis error");
    error_info_ = EXEC_REDIS_ERROR;
    return 0;
  }
  LOG_DEBUG2("total: %s", val);
  return (val == NULL) ? 0 : atoll(val);
}

std::string DbRedis::GetStockUpDown(const std::string &code) {
  base_storage::DictionaryStorageEngine* redis = GetRedis();
  if (NULL == redis) {
    error_info_ = CONNECT_ERROR;
    return "0";
  }

  char buf[64];
  time_t now = time(NULL);
  strftime(buf,sizeof(buf),"newstock:%Y-%m-%d",localtime(&now));
  MapStrToStr ret;
  bool r = redis->GetSortedSet(buf, \
                                19, \
                               "0", \
							"2779", \
                                 1, \
                              ret);
  std::string updown;
  r = tools::MapFind(ret, code, &updown);

  if (r == false) return "0";

  if (atof(updown.c_str()) >= 0.0001) {
    double temp = atof(updown.c_str()) * 100;
    std::stringstream os("");
    os << temp;
    updown = os.str();
    updown = updown.substr(0, 4);
    updown += "%";
  } else {
    updown = "0";
  }

  return updown;
}

}  // namespace db
