#pragma once

#include "common.h"
#include "dbf_worker.h"
#include "lock.h"

#include <thread>
#include <map>
#include <condition_variable>

class DbfReader {
public:
	explicit DbfReader(std::string& filename);
	~DbfReader();

private:
	bool OnInit();

public:
	std::thread Start();
	void Run();

private:
	bool OnTimeReadDbf(std::string& redis_command);
	void SignalRedisTask(std::string& redis_command);
	void BackupDbfFile();
	bool DbfReader::LoadStockCode(time_t now, std::string& exchange_type);

	bool CheckIsPing(time_t curr_time);

private:
	std::string filename_;			//dbf文件
	std::string exchange_type_;		//交易所类型
	int wait_interval_;				//读取间隔

	base_logic::DbfShared shared_;	//共享结构

	std::string table_head_name_;
	std::string curr_read_time_;

	int next_ping_singal_;
	int next_reload_stock_time_;
	std::map<std::string, bool> stock_code_map_;

	int worker_num_;
	std::thread* thread_workers_;
	std::vector<DbfWorker*> dbf_workers_;

	LockFileGuard lock_file_guard_;
	std::mutex mutex_;
	std::condition_variable condition_variable_;
};
