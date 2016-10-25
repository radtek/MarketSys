#include "stdafx.h"
#include "dbf_worker.h"
#include "redis_client.h"

DbfWorker::DbfWorker(base_logic::DbfShared& shared,
                     std::string& filename)
	: shared_(shared),
	  filename_(filename) {
	if(!OnInit()) {
		MessageBox(NULL, TEXT("ERROR"), TEXT("DbfWorker Init Error"), MB_OK);
		exit(-1);
	}
}

DbfWorker::~DbfWorker() {

}

bool DbfWorker::OnInit() {
	const base_logic::ConnAddr& addr_redis = Configure::GetRef().GetRedisInfo();
	redis_.reset(new RedisClient(const_cast<base_logic::ConnAddr&>(addr_redis)));
	return true;
}

std::thread DbfWorker::Start() {
	return std::thread(std::bind(&DbfWorker::Run, this));
}

void DbfWorker::Run() {
	for (;;) {
		WaitForSingleObject(shared_.nstored_, INFINITE);
		std::unique_lock<std::mutex> lock(shared_.mutex_);
		if(!shared_.redis_wait_excute_tasks_.empty()) {
			base_logic::RedisTaskPtr redis_task_ptr = shared_.redis_wait_excute_tasks_.front();
			bool r = redis_->Hset(redis_task_ptr->tablename_,
				redis_task_ptr->rowkey_,
				redis_task_ptr->value_);
			if (r) {
				shared_.redis_wait_excute_tasks_.pop_front();
				ReleaseSemaphore(shared_.nempty_, 1, NULL);
			}
			else {
				ReleaseSemaphore(shared_.nstored_, 1, NULL);
			}
		}
	}
}
