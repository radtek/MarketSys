// Copyright (c) 2015-2015 The restful Authors. All rights reserved.
// Created on: 2015/11/24 Author: jiaoyongqing

#include <string>
#include <sstream>
#include <map>

#include "global/global.h"
#include "net/typedef.h"

namespace global {
  GetInfo *GetInfo::instance_ = NULL;

  bool GetInfo::CheckMulCodeIfExist(const ContainerStr &container) {
    ContainerStr::const_iterator it = container.begin();
    bool r = false;
    for (; it != container.end(); ++it) {
      r = CheckSingleCodeIfExist(*it);
      if (false == r) return false;
    }

    return true;
  }
}  //  namespace global
