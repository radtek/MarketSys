#pragma once

#include <string>
#include <condition_variable>
#include <list>

namespace base_logic {

#define SH_EXCHANGE		"001002"
#define SZ_EXCHANGE		"001003"

struct RedisTask {
	std::string tablename_;
	std::string rowkey_;
	std::string value_;
};
using RedisTaskPtr = std::shared_ptr<RedisTask>;

typedef struct {
	std::string st_address;
	std::string st_port;
	std::string st_username;
	std::string st_password;
	std::string st_dbname;
} ConnAddr;


typedef std::list<RedisTaskPtr> REDIS_WAIT_EXCUTE_TASKS;


struct DbfShared {
	HANDLE nempty_;
	HANDLE nstored_;
	std::mutex mutex_;
	REDIS_WAIT_EXCUTE_TASKS redis_wait_excute_tasks_;
};

} /*namespace base_logic*/
