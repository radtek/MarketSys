//  Copyright (c) 2015-2015 The KID Authors. All rights reserved.
//  Created on: 2016/10/13 Author: zjc

#define DEBUG
#include "stock_logic.h"

#include <algorithm>

#include "stock_db.h"
#include "logic/logic_comm.h"
#include "stock_util.h"

#define DEFAULT_CONFIG_PATH     "./plugins/stock/stock_config.xml"

#define TIMER_SCAN_REDIS  0

namespace stock_logic {


StockLogic* StockLogic::instance_ = NULL;
const char* const StockLogic::hkey_prefix_[DataEngine::MAX_TYPE] = {
    "sh_stock_",
    "sz_stock_"
};

StockLogic::StockLogic() {
  if (!Init())
    assert(0);
}

StockLogic::~StockLogic() {

}

bool StockLogic::Init() {
  config::FileConfig* conf = config::FileConfig::GetFileConfig();
  if (NULL == conf) {
    return false;
  }
  std::string path = DEFAULT_CONFIG_PATH;
  if (!conf->LoadConfig(path)) {
    return false;
  }

  market_time_ = -1;
  db_ = new StockDB(conf);
  redis_.InitParam(conf->redis_list_);

  for (size_t i = 0; i < DataEngine::MAX_TYPE; ++i) {
    data_[i].Init((DataEngine::Type)i, hkey_prefix_[i], db_, &redis_);
  }
  return true;
}

StockLogic*
StockLogic::GetInstance() {
  if (instance_ == NULL)
    instance_ = new StockLogic();
  return instance_;
}

void StockLogic::FreeInstance() {
  delete instance_;
  instance_ = NULL;
}

bool StockLogic::OnStockConnect(struct server *srv, const int socket) {
  return true;
}

bool StockLogic::OnStockMessage(struct server *srv, const int socket,
                                const void *msg, const int len) {
  return true;
}

bool StockLogic::OnStockClose(struct server *srv, const int socket) {
  return true;
}

bool StockLogic::OnBroadcastConnect(struct server *srv, const int socket,
                                    const void *msg, const int len) {
  return true;
}

bool StockLogic::OnBroadcastMessage(struct server *srv, const int socket,
                                    const void *msg, const int len) {
  return true;
}

bool StockLogic::OnBroadcastClose(struct server *srv, const int socket) {
  return true;
}

bool StockLogic::OnIniTimer(struct server *srv) {
  srv->add_time_task(srv, "stock", TIMER_SCAN_REDIS, 5, -1);
  return true;
}

bool StockLogic::OnTimeout(struct server *srv, char *id, int opcode, int time) {
  switch (opcode) {
    case TIMER_SCAN_REDIS:
      ScanRedis();
      break;
  }
  return true;
}

void StockLogic::ScanRedis() {
  if (!(StockUtil::instance_->is_trading_day()
      && data_[DataEngine::SH].is_work_time())) {
    market_time_ = -1;
    return ;
  }

  for (size_t i = 0; i < DataEngine::MAX_TYPE; ++i) {
    data_[i].OnTime();
  }
  int sh_mtime = data_[DataEngine::SH].market_time();
  int sz_mtime = data_[DataEngine::SZ].market_time();
  int mtime = std::min(sh_mtime, sz_mtime);

  bool b = true;
  if (mtime > market_time_) {
    market_time_ = mtime;
    LOG_MSG2("update realtime data, market_time: %d", mtime);
    for (size_t i = 0; i < DataEngine::MAX_TYPE; ++i) {
      b &= data_[i].UpdateRealtimeData(mtime);
    }

    if (b) {
      db_->DelOldRealtimeData(StockUtil::instance_->to_timestamp(mtime));
    }
  }
}

} /* namespace stock_logic */
