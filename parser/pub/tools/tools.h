//  Copyright (c) 2015-2015 The restful Authors. All rights reserved.
//  Created on: 2015/11/24 Author: jiaoyongqing

#ifndef _HOME_RUNNER_JINDOWIN_PUB_TOOLS_TOOLS_H_
#define _HOME_RUNNER_JINDOWIN_PUB_TOOLS_TOOLS_H_

#include<time.h>

#include<string>
#include <vector>
#include <map>
#include <algorithm>

#include "logic/logic_basic_info.h"
#include "net/typedef.h"
#include "net/http_data.h"
namespace tools {

  //  得到省份字符串
std::string GetProvinceString(int province);

time_t StrToTime(const char *Data);

std::string GetTimeKey(time_t time);

time_t TodayStartTime();

time_t CurrentTime();

std::string::size_type FindNth(const std::string &str, \
                         std::string::size_type start, \
                                              int len, \
                                              char ch, \
                                              int num);

std::string GetToken(int64 user_id, std::string &token);

bool CheckToken(int64 user_id, std::string &token);

bool check_id_token(NetBase*  value,const int socket);

bool check_userid_if_in_sql(NetBase*  value,const int socket);

bool CheckUserIdAndToken(NetBase*  value,const int socket);

std::string DeleteSet(const std::string &set_one, \
                      const std::string &set_two, \
                                  char separator);


bool IfSetOneIsInSetTwo(const std::string &set_one, \
                        const std::string &set_two, \
                                    char separator);
std::string MergeSet(const std::string &set_one, \
                     const std::string &set_two, \
                                 char separator);

void MapAdd(std::map<std::string, int64> *map, \
          const std::string &key, int64 value);

void ListGroup(const ContainerStr &l, int group_size, \
            char separator, ContainerStr *const out);

std::string TeaEncode(const std::string src);
std::string TeaDecode(const std::string src);

template<class T>
void SeparatorStr(const std::string &instr, \
  char separator, T *l, bool repeat = true) {
  std::string str(instr);

  if (str == "") return;

  str = std::string(1, separator) + str;

  if (str[str.length() - 1] != separator) {
    str = str + std::string(1, separator);
  }

  MapStrToInt64 map_repeat;
  map_repeat.clear();
  MapStrToInt64::iterator tmp_it;
  std::string value;
  std::string::size_type start = 0;
  std::string::size_type end = str.find(separator, start + 1);
  while (end != std::string::npos) {
    if (end - start == 1) {
      value = "";
    } else {
      value = str.substr(start + 1, end - (start + 1));
    }

    start = end;
    end = str.find(separator, start + 1);

    if (repeat == false) {
      tmp_it = map_repeat.find(value);
      if (tmp_it == map_repeat.end()) {
        map_repeat[value] = 1;
      } else {
        continue;
      }
    }

    l->push_back(value);
  }
}

template<class M, class L, class R>
bool MapFind(M map, L key, R *value) {
  if (map.find(key) == map.end()) {
    return false;
  } else {
    *value = map[key];
    return true;
  }
}

template <class T, class F>
void HandleHeap(T min_heap[], int *const heap_count, \
                    const T &temp, int heap_max_num) {
  if (*heap_count < heap_max_num) {
    min_heap[*heap_count] = temp;
    ++(*heap_count);
    std::push_heap(&min_heap[0], &min_heap[*heap_count], F());
  } else {
    if (F()(temp, min_heap[0])) {
      std::pop_heap(&min_heap[0], &min_heap[heap_max_num], F());
      min_heap[heap_max_num - 1] = temp;
      std::push_heap(&min_heap[0], &min_heap[heap_max_num], F());
    }
  }
}

std::vector<std::string> Split(std::string str, std::string pattern);

std::string TimeFormat(int64 time, const char* format);

void replace_all(std::string *str, const std::string &old_value, \
                                     const std::string &new_value);

void replace_all_distinct(std::string *str, const std::string &old_value, \
                                             const std::string &new_value);

void ReplaceBlank(std::string *str);

}  // namespace tools

#endif  //  _HOME_RUNNER_JINDOWIN_PUB_TOOLS_TOOLS_H_
