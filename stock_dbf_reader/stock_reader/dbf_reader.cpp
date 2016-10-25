#include "stdafx.h"
#include "dbf_reader.h"
#include "configure.h"
#include "mysql.hpp"
#include "dbf_worker.h"
#include "dbf.h"
#include "lock.h"

#define DEFAULT_WAIT_TIME	3
#define DEFAULT_PING_WAIT	15
#define ONE_HOUR_SECONDS	(1*60*60)

DbfReader::DbfReader(std::string& filename)
	: filename_(filename) {
	if (!OnInit()) {
		MessageBox(NULL, TEXT("ERROR"), TEXT("DbfReader Init Error"), MB_OK);
		exit(-1);
	}
}

DbfReader::~DbfReader() {
	for (int i = 0; i < worker_num_; ++i) {
		thread_workers_[i].join();
		delete dbf_workers_[i];
	}
	delete[] thread_workers_;
}

bool DbfReader::OnInit() {
	//获取扫描时间
	int wait = DEFAULT_WAIT_TIME;
	if (std::string::npos != filename_.find("SZ.DBF")) {
		wait = Configure::GetPtr()->GetSZDBFWait();
		table_head_name_ = "sz_stock_";
		exchange_type_ = SZ_EXCHANGE;
	}
	else if (std::string::npos != filename_.find("SH.DBF")) {
		wait = Configure::GetPtr()->GetSHDBFWait();
		table_head_name_ = "sh_stock_";
		exchange_type_ = SH_EXCHANGE;
	}
	wait_interval_ = wait > DEFAULT_WAIT_TIME ? DEFAULT_WAIT_TIME : wait;

	//初始化信号量
	shared_.nempty_ = CreateSemaphore(NULL, 50, 50, NULL);
	shared_.nstored_ = CreateSemaphore(NULL, 0, 50, NULL);

	//载入股票列表
	if (!LoadStockCode(time(NULL), exchange_type_)) {
		LOG(ERROR) << "DbfReader Start LoadStockCode error";
		return false;
	}

	//启动工作线程
	worker_num_ = Configure::GetPtr()->GetThreadPoolNum();
	thread_workers_ = new std::thread[worker_num_];
	for (int i = 0; i < worker_num_; ++i) {
		DbfWorker* worker = new DbfWorker(shared_, filename_);
		thread_workers_[i] = worker->Start();
		dbf_workers_.push_back(worker);
	}
	return true;
}

std::thread DbfReader::Start() {
	std::cout << "create proccess " << filename_ << std::endl;
	return std::thread(std::bind(&DbfReader::Run, this));
}

void DbfReader::Run() {
	using namespace std::chrono;
	while (true) {
		std::unique_lock<std::mutex> lock(mutex_);
		if (condition_variable_.wait_for(lock, seconds(wait_interval_), [this] {
			LockFileGuard lgd; return lgd.CheckLock(filename_); })) {
			std::string redis_commad;
			if (OnTimeReadDbf(redis_commad)) {
				SignalRedisTask(redis_commad);
				BackupDbfFile();
			}
			else {
				time_t now = time(NULL);
				CheckIsPing(now);
				LoadStockCode(now, exchange_type_);
			}
		}
		sleep(wait_interval_);
	}
}

bool DbfReader::OnTimeReadDbf(std::string& redis_command) {
	bool r = false;
	dbf::DbfPtr dbf(new ::dbf::Dbf());
	r = dbf->Open(filename_);
	if (!r) {
		MessageBoxA(NULL, "dbf open file error", "ERROR", MB_OK);
		return false;
	}
	r = dbf->HandleDbfRecords(redis_command, stock_code_map_, curr_read_time_);
	return r;
}

void DbfReader::SignalRedisTask(std::string& redis_command) {
	if( !curr_read_time_.empty() && !redis_command.empty()) {
		WaitForSingleObject(shared_.nempty_, INFINITE);
		std::unique_lock<std::mutex> lock(shared_.mutex_);
		base_logic::RedisTaskPtr redis_task_ptr(new base_logic::RedisTask());
		redis_task_ptr->tablename_ = table_head_name_ + base_logic::Utils::GetTimeOfDay();
		redis_task_ptr->rowkey_ = curr_read_time_;
		redis_task_ptr->value_ = redis_command;
		shared_.redis_wait_excute_tasks_.push_back(redis_task_ptr);
		ReleaseSemaphore(shared_.nstored_, 1, NULL);
	}
}

void DbfReader::BackupDbfFile() {
	std::string back_file_name_;
	std::string backup_path = Configure::GetPtr()->GetDbfPath() + "\\";
	if (std::string::npos != filename_.find("SH")) {
		backup_path += "SH_" + base_logic::Utils::GetTimeOfDay() + "_bak" + "\\";
		back_file_name_ = backup_path + "SH_" + curr_read_time_ + ".DBF";
	}
	else {
		backup_path += "SZ_" + base_logic::Utils::GetTimeOfDay() + "_bak" + "\\";
		back_file_name_ = backup_path + "SZ_" + curr_read_time_ + ".DBF";
	}
	if (access(backup_path.c_str(), 0) == -1) {
		CreateDirectoryA(backup_path.c_str(), NULL);
	}
	CopyFileA(filename_.c_str(), back_file_name_.c_str(), FALSE);
}

bool DbfReader::LoadStockCode(time_t now, std::string& exchange_type) {
	if (now > next_reload_stock_time_) {
		next_reload_stock_time_ = now + ONE_HOUR_SECONDS;

		const base_logic::ConnAddr& conn_addr = Configure::GetRef().GetMySqlInfo();
		mysql conn(const_cast<base_logic::ConnAddr&>(conn_addr));

		std::string query;
		typedef std::tuple<std::string> StockTuple;
		std::vector<StockTuple> stock_code_vec;
		if (SH_EXCHANGE == exchange_type) {
			query = "SELECT code FROM test.`algo_get_stock_basics` WHERE CODE LIKE '60%' OR CODE LIKE '900%' OR CODE LIKE '500%';";
		}else if (SZ_EXCHANGE == exchange_type) {
			query = "SELECT code FROM test.`algo_get_stock_basics` WHERE CODE LIKE '00%' OR CODE LIKE '200%' OR CODE LIKE '300%' OR CODE LIKE '184%';";
		}
		conn.runQuery(&stock_code_vec, query.c_str());
		if (stock_code_vec.empty()) {
			return false;
		}
		stock_code_map_.clear();
		std::vector<StockTuple>::iterator iter(stock_code_vec.begin());
		for (; stock_code_vec.end() != iter; ++iter) {
			std::tuple<std::string>& stock_tuple = (*iter);
			std::string stock_code = std::get<0>(stock_tuple);
			stock_code_map_[stock_code] = true;
		}
		LOG(INFO) << filename_ << " LoadDb stock_code size " << stock_code_map_.size();
		return true;
	}
	return false;
}

bool DbfReader::CheckIsPing(time_t curr_time) {
	if (curr_time > next_ping_singal_) {
		next_ping_singal_ = curr_time + DEFAULT_PING_WAIT;
		// all woker ping
		std::unique_lock<std::mutex> lock(shared_.mutex_);
		for (int i = 0; i < worker_num_; ++i) {
			dbf_workers_[i]->ping();
		}
	}
	return true;
}
