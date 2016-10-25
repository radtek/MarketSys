//  Copyright (c) 2015-2015 The restful Authors. All rights reserved.
//  Created on: 2015/11/24 Author: jiaoyongqing

#include "tea/tea.h"

#include <malloc.h>
#include <stdlib.h>
#include <memory.h>
#include <stdio.h>

#include <iostream>

uint32_t key[4] = {0x04, 0x03, 0x02, 0x01};

// jiami
void encrypt(uint32_t v[], uint32_t k[]) {
  uint32_t v0 = v[0], v1 = v[1], sum = 0, i;
  uint32_t delta = 0x9e3779b9;
  uint32_t k0 = k[0], k1 = k[1], k2 = k[2], k3 = k[3];
  for (i=0; i < 32; i++) {
    sum += delta;
    v0 += ((v1<<4) + k0) ^ (v1 + sum) ^ ((v1>>5) + k1);
    v1 += ((v0<<4) + k2) ^ (v0 + sum) ^ ((v0>>5) + k3);
  }
  v[0] = v0; v[1] = v1;
}

// jiemi
void decrypt(uint32_t v[], uint32_t k[]) {
  uint32_t v0 = v[0], v1 = v[1], sum = 0xC6EF3720, i;
  uint32_t delta = 0x9e3779b9;
  uint32_t k0 = k[0], k1 = k[1], k2 = k[2], k3 = k[3];
  for (i = 0; i < 32; i++) {
    v1 -= ((v0<<4) + k2) ^ (v0 + sum) ^ ((v0>>5) + k3);
    v0 -= ((v1<<4) + k0) ^ (v1 + sum) ^ ((v1>>5) + k1);
    sum -= delta;
  }
  v[0] = v0; v[1] = v1;
}

bool StrEn(struct tea_data * td) {
  if (td == NULL || td->d == NULL) {
    return false;
  }

  if (td->l % 8 != 0) {
    size_t new_l = ((td->l - 1) / 8 + 1) * 8;
    void *new_d = reinterpret_cast<void *>(malloc(new_l));
    memset(new_d, 0, new_l);

    for (size_t i = 0; i < td->l; ++i) {
      (reinterpret_cast<char *>(new_d))[i] = \
            (reinterpret_cast<char *>(td->d))[i];
    }

    td->d = reinterpret_cast<unsigned char *>(new_d);
    td->l = new_l;
  }

  uint32_t *p = reinterpret_cast<uint32_t*>(td->d);
  size_t l = td->l / 4;
  for (int i = 0; i < l; i += 2) {
    encrypt(&p[i], key);
  }

  return true;
}

bool StrDe(struct tea_data *td) {
  if (td == NULL || td->d == NULL || td->l % 8 != 0) {
    return false;
  }

  uint32_t *p = reinterpret_cast<uint32_t*>(td->d);
  size_t l = td->l / 4;

  for (int i = 0; i < l; i += 2) {
    decrypt(&p[i], key);
  }

  return true;
}
