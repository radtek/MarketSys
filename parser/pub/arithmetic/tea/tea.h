// Copyright (c) 2015-2015 The restful Authors. All rights reserved.
// Created on: 2015/11/24 Author: jiaoyongqing

#ifndef _HOME_RUNNER_JINDOWIN_PUB_ARITHMETIC_TEA_TEA_H_
#define _HOME_RUNNER_JINDOWIN_PUB_ARITHMETIC_TEA_TEA_H_

#include <stdint.h>

#include <string>
#include <iostream>

struct tea_data {
  void *d;
  size_t l;
};

bool StrEn(struct tea_data * td);

bool StrDe(struct tea_data *td);

#endif  //  _HOME_RUNNER_JINDOWIN_PUB_ARITHMETIC_TEA_TEA_H_
