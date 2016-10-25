#pragma once

#include "stdafx.h"
#include <string>
#include <time.h>
#include <sstream>
#include <chrono>

namespace base_logic {

class Utils {
public:
	static void repaceTab(std::string& str) {
		for (unsigned int i = 0; i < str.length(); ++i) {
			if (str[i] == '\t' || str[i] == '\b' || str[i] == '\v'
				|| str[i] == '\r') {
				str[i] = ' ';
			}
		}
	}

	static void trim(std::string& str) {
		str.erase(str.find_last_not_of(' ') + 1, std::string::npos);
		str.erase(0, str.find_first_not_of(' '));
	}

	template <typename T>
	static std::string convertString(const T& rhs) {
		std::stringstream ss;
		ss << rhs;
		return ss.str();
	}

	template <typename T>
	static double convertDouble(const T& rhs) {
		std::stringstream ss;
		ss << rhs;
		double result;
		ss >> result;
		return result;
	}

	static std::string GetTimeOfDay() {
		time_t current_time = time(NULL);
		struct tm* current_tm = localtime(&current_time);
		char str_today[20];
		sprintf(str_today, "%d%02d%02d", current_tm->tm_year + 1900,
		        current_tm->tm_mon + 1, current_tm->tm_mday);
		return std::string(str_today);
	}

	static std::string GetCurrTime() {
		time_t current_time = time(NULL);
		struct tm* current_tm = localtime(&current_time);
		char str_time[20];
		sprintf(str_time, "%02d:%02d:%02d", current_tm->tm_hour,
		        current_tm->tm_min, current_tm->tm_sec);
		return std::string(str_time);
	}

	static bool is_trading_time() {
		time_t current_time = time(NULL);
		struct tm* current_tm = localtime(&current_time);
		if (0 == current_tm->tm_wday || 6 == current_tm->tm_wday)
			return false;
		char str_hour_time[20];
		sprintf(str_hour_time, "%02d%02d%02d", current_tm->tm_hour,
		        current_tm->tm_min, current_tm->tm_sec);
		if (strcmp(str_hour_time, "092500") < 0
			|| strcmp(str_hour_time, "150000") > 0
			|| (strcmp(str_hour_time, "113000") > 0
				&& strcmp(str_hour_time, "130000") < 0)) {
			return false;
		}
		return true;
	}

	static unsigned long long GetSystemTime() {
		return std::chrono::duration_cast<std::chrono::seconds>(
			std::chrono::system_clock::now().time_since_epoch()).count();
	}
};

} /*namespace base_logic*/
