// Copyright (c) 2015-2015 The restful Authors. All rights reserved.
// Created on: 2015/11/24 Author: jiaoyongqing

#ifndef _HOME_RUNNER_NORTHSEA_RESTFUL_PUB_GLOBAL_GLOBAL_H_
#define _HOME_RUNNER_NORTHSEA_RESTFUL_PUB_GLOBAL_GLOBAL_H_

#include<mysql.h>
#include<string>

#include "net/typedef.h"
#include "db/db_comm.h"
#include "tools/tools.h"
#include "logic/logic_comm.h"

namespace global {
class GetInfo {
 public:
  virtual ~GetInfo() {}

 private:
  GetInfo() {}

class crelease {
  ~crelease() {
    if (instance_ != NULL) {
      delete instance_;
      instance_ = NULL;
    }
  }
};

 private:
  void init() {
    db::DbSql sql;
    sql.GetAllNameCode();
    MYSQL_ROW rows;
    while (NULL != (rows = sql.NextRecord())) {
      get_code_by_name_[rows[1]] = rows[0];
      get_name_by_code_[rows[0]] = rows[1];
      codes_.push_back(rows[0]);
      names_.push_back(rows[1]);
      jp_.push_back(rows[2]);
      qp_.push_back(rows[3]);
    }

    sql.GetHy();
    while (NULL != (rows = sql.NextRecord())) {
      hy_.push_back(rows[0]);
      hy_jp_.push_back(rows[1]);
      hy_qp_.push_back(rows[2]);
    }

    sql.GetGn();
    while (NULL != (rows = sql.NextRecord())) {
      gn_.push_back(rows[0]);
      gn_jp_.push_back(rows[1]);
      gn_qp_.push_back(rows[2]);
    }
  }

 public:
  static GetInfo* GetInstance() {
    if (NULL == instance_) {
      instance_ = new GetInfo();
      instance_->init();
    }

    return instance_;
  }

  inline std::string GetCodeByName(const std::string &code) {
    return get_code_by_name_[code];
  }

  inline std::string GetNameByCode(const std::string &name) {
    return get_name_by_code_[name];
  }

  inline ContainerStr get_codes() { return codes_; }

  inline ContainerStr get_names() { return names_; }
  inline ContainerStr get_jp() { return jp_; }
  inline ContainerStr get_qp() { return qp_; }

  inline ContainerStr get_hy() { return hy_; }
  inline ContainerStr get_hy_jp() { return hy_jp_; }
  inline ContainerStr get_hy_qp() { return hy_qp_; }

  inline ContainerStr get_gn() { return gn_; }
  inline ContainerStr get_gn_jp() { return gn_jp_; }
  inline ContainerStr get_gn_qp() { return gn_qp_; }

  inline bool CheckSingleCodeIfExist(const std::string &code) {
    std::string name;
    return tools::MapFind<MapStrToStr, std::string, std::string>\
                                (get_name_by_code_, code, &name);
  }

  bool CheckMulCodeIfExist(const ContainerStr &container);

 private:
  static crelease release_;
  static GetInfo *instance_;

 private:
  MapStrToStr get_code_by_name_;
  MapStrToStr get_name_by_code_;
  ContainerStr codes_;
  ContainerStr names_;
  ContainerStr jp_;
  ContainerStr qp_;

  ContainerStr hy_;
  ContainerStr hy_jp_;
  ContainerStr hy_qp_;

  ContainerStr gn_;
  ContainerStr gn_jp_;
  ContainerStr gn_qp_;
};

}  // namespace global

#endif  //  _HOME_RUNNER_NORTHSEA_RESTFUL_PUB_GLOBAL_GLOBAL_H_
