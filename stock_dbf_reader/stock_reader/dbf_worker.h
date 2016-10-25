#pragma once
#include "common.h"
#include "redis_client.h"

class DbfWorker {
public:
	explicit DbfWorker(base_logic::DbfShared& shared,
	                   std::string& filename);
	~DbfWorker();

private:
	bool OnInit();

public:
	std::thread Start();
	void Run();

	void ping() {
		redis_->PING();
	}

private:
	std::string& filename_;
	base_logic::DbfShared& shared_;
	std::shared_ptr<RedisClient> redis_;
};
