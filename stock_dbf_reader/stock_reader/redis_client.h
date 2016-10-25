#pragma once

#include "hiredis.h"
#include "configure.h"

class RedisClient {
public:
	RedisClient(base_logic::ConnAddr& conn);
	~RedisClient();

private:
	bool OnInit();
	bool checkReply(const redisReply* reply);

public:
	bool PING();
	/*Command*/
	bool Command(std::string& command);

	/*STRING*/
	bool Append(std::string& key, std::string& value);

	/*HASH*/
	bool Hset(std::string& key, std::string& field, std::string& value); 

private:
	base_logic::ConnAddr& conn_;
	redisContext* redisContext_;
};
