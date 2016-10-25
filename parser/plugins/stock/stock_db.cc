//  Copyright (c) 2015-2015 The KID Authors. All rights reserved.
//  Created on: 2016年10月13日 Author: zjc

#include "stock_db.h"

#include <mysql.h>
#include <sstream>

#include "logic/logic_comm.h"

namespace stock_logic {

StockDB::StockDB(config::FileConfig* config) {
  mysql_engine_ = base_logic::DataEngine::Create(MYSQL_TYPE);
  mysql_engine_->InitParam(config->mysql_db_list_);
}

StockDB::~StockDB() {
  if (mysql_engine_) {
    delete mysql_engine_;
    mysql_engine_ = NULL;
  }
}

std::string StockDB::CodeRealtimeInfo::Serialized() const {
  std::ostringstream oss;
  oss << "'" << code << "'" << ","
      << "'" << name << "'" << ","
      << change_percent << ","
      << price << ","
      << open << ","
      << high << ","
      << low << ","
      << settlement << ","
      << type << ","
      << market_time << ","
      << volume << ","
      << amount;
  return oss.str();
}

bool StockDB::ClearTodayStockAllInfo() {
  bool r = false;
  base_logic::DictionaryValue* dict = new base_logic::DictionaryValue();
  std::string sql = "call proc_ClearTodayStockAllInfo()";
  LOG_MSG2("sql=%s", sql.c_str());
  dict->SetString(L"sql", sql);
  r = mysql_engine_->WriteData(0, (base_logic::Value*) dict);
  delete dict;
  dict = NULL;
  if (!r)
    return false;
  return true;
}

bool StockDB::DelPreTradeData() {
  bool r = false;
  base_logic::DictionaryValue* dict = new base_logic::DictionaryValue();
  std::string sql = "call DelPreTradeData()";
  LOG_MSG2("sql=%s", sql.c_str());
  dict->SetString(L"sql", sql);
  r = mysql_engine_->WriteData(0, (base_logic::Value*) dict);
  delete dict;
  dict = NULL;
  if (!r)
    return false;
  return true;
}

bool StockDB::DelOldRealtimeData(time_t mtime) {
  bool r = false;
  base_logic::DictionaryValue* dict = new base_logic::DictionaryValue();
  std::string sql;
  std::ostringstream oss;
  oss << "call proc_DeleteOldRealtimeData(" << mtime << ")";
  sql = oss.str();
  LOG_MSG2("sql=%s", sql.c_str());
  dict->SetString(L"sql", sql);
  r = mysql_engine_->WriteData(0, (base_logic::Value*) dict);
  delete dict;
  dict = NULL;
  if (!r)
    return false;
  return true;
}

bool StockDB::UpdateTodayStockAllInfo(const CodeInfoArray& data) {
  if (data.empty()) {
    return false;
  }

  bool r = false;
  base_logic::DictionaryValue* dict = new base_logic::DictionaryValue();
  std::string sql;
  std::stringstream os;
  os << "REPLACE INTO algo_today_stock_all_info("
     "`CODE`, "
     "`NAME`, "
     "`changepercent`, "
     "`trade`, "
     "`OPEN`, "
     "`high`, "
     "`low`, "
     "`settlement`, "
     "`TYPE`, "
     "`time`, "
     "`volume`, "
     "`amount`) ";
  os << "VALUES(" << data[0].Serialized() << ")";
  for (size_t i = 1; i < data.size(); ++i) {
    os << ",(" << data[i].Serialized() << ")";
  }
//  os << "call proc_UpdateTodayStockAllInfo("
//     << "'" << info.code << "'" << ","
//     << "'" << info.name << "'" << ","
//     << info.change_percent << ","
//     << info.price << ","
//     << info.open << ","
//     << info.high << ","
//     << info.low << ","
//     << info.settlement << ","
//     << info.type << ","
//     << info.market_time << ","
//     << info.volume << ","
//     << info.amount
//     << ")";
  sql = os.str();
  LOG_MSG2("sql=%s", sql.c_str());
  dict->SetString(L"sql", sql);
  r = mysql_engine_->WriteData(0, (base_logic::Value*) dict);
  delete dict;
  dict = NULL;
  if (!r)
    return false;
  return true;
}

bool StockDB::UpdateTodayStock(const CodeInfoArray& data) {
  if (data.empty()) {
    return false;
  }

  bool r = false;
  base_logic::DictionaryValue* dict = new base_logic::DictionaryValue();
  std::string sql;
  std::stringstream os;
  os << "REPLACE INTO algo_today_stock("
     "`CODE`, "
     "`NAME`, "
     "`changepercent`, "
     "`trade`, "
     "`OPEN`, "
     "`high`, "
     "`low`, "
     "`settlement`, "
     "`TYPE`, "
     "`time`, "
     "`volume`, "
     "`amount`) ";
  os << "VALUES(" << data[0].Serialized() << ")";
  for (size_t i = 1; i < data.size(); ++i) {
    os << ",(" << data[i].Serialized() << ")";
  }
//  os << "call proc_UpdateTodayStockAllInfo("
//     << "'" << info.code << "'" << ","
//     << "'" << info.name << "'" << ","
//     << info.change_percent << ","
//     << info.price << ","
//     << info.open << ","
//     << info.high << ","
//     << info.low << ","
//     << info.settlement << ","
//     << info.type << ","
//     << info.market_time << ","
//     << info.volume << ","
//     << info.amount
//     << ")";
  sql = os.str();
  LOG_MSG2("sql=%s", sql.c_str());
  dict->SetString(L"sql", sql);
  r = mysql_engine_->WriteData(0, (base_logic::Value*) dict);
  delete dict;
  dict = NULL;
  if (!r)
    return false;
  return true;
}

} /* namespace stock_logic */
