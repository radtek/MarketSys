#include "stdafx.h"
#include "redis_client.h"
#include "redis_exception.h"
#include "common.h"

#include <windows.h>
#include <string.h>
#include <stdexcept>

#define PREPARE_REDIS_CONTEXT_EXCEPTION(message)  std::string(__FUNCTION__)+ \
                                    "|"+ (redisContext_->errstr == NULL ? "" : redisContext_->errstr)+ \
                                    "|" + std::string(message)

#define PREPARE_REDIS_EXCEPTION(message)  std::string(__FUNCTION__)+ \
                                    "|"+ (redisContext_->errstr == NULL ? "" : redisContext_->errstr)+ \
                                    "|" +(reply->str == NULL ? "" : reply->str)+ \
                                    "|" + std::string(message)

RedisClient::RedisClient(base_logic::ConnAddr& conn)
	: conn_(conn) {
	if (!OnInit()) {
		MessageBox(NULL, TEXT("Redis connected error"), TEXT("ERROR"), MB_OK);
		exit(-1);
	}
}

RedisClient::~RedisClient() {

}

bool RedisClient::OnInit() {
	try {
		struct timeval timeout = { 0, 1000000 }; // 1s
		redisContext_ = redisConnectWithTimeout(conn_.st_address.c_str(), atoi(conn_.st_port.c_str()), timeout);
		if (!redisContext_ || redisContext_->err) {
			RedisException e(PREPARE_REDIS_CONTEXT_EXCEPTION("connect failed!"));
			if (redisContext_) {
				redisFree(redisContext_);
				redisContext_ = NULL;
			}
			throw e.what();
		}
		redisReply* reply = static_cast<redisReply*>(redisCommand(redisContext_, "AUTH %s", conn_.st_password.c_str()));
		if (!checkReply(reply)) {
			RedisException e(PREPARE_REDIS_EXCEPTION());
			if (reply)
				freeReplyObject(reply);
			throw e.what();
		}
		reply = static_cast<redisReply*>(redisCommand(redisContext_, "SELECT %d", atoi(conn_.st_dbname.c_str())));
		if (!checkReply(reply)) {
			RedisException e(PREPARE_REDIS_EXCEPTION());
			if (reply)
				freeReplyObject(reply);
			throw e.what();
		}
	}
	catch (const std::string& e) {
		MessageBox(NULL, TEXT("ERROR"), LPCWSTR(e.c_str()), MB_OK);
		return false;
	}
	return true;
}

bool RedisClient::checkReply(const redisReply* reply) {
	if (reply == NULL)
		return false;

	switch (reply->type) {
	case REDIS_REPLY_STRING: return true;
	case REDIS_REPLY_ARRAY: return true;
	case REDIS_REPLY_INTEGER: return true;
	case REDIS_REPLY_NIL: return false;
	case REDIS_REPLY_STATUS: return (strcasecmp(reply->str, "OK") == 0) ? true : false;
	case REDIS_REPLY_ERROR: return false;
	default: return false;
	}
	return false;
}

bool RedisClient::PING() {
	std::string ping_msg = "PING";
	redisReply* reply = static_cast<redisReply*>(redisCommand(redisContext_, ping_msg.c_str()));
	if (reply == NULL)
		return false;

	if(strcasecmp(reply->str, "PONG") != 0) {
		OnInit();
	}

	freeReplyObject(reply);
	return true;
}

bool RedisClient::Command(std::string& command) {
	redisReply* reply = static_cast<redisReply*>(redisCommand(redisContext_, command.c_str()));
	if (reply == NULL)
		return false;

	if (!checkReply(reply)) {
		RedisException e(PREPARE_REDIS_EXCEPTION());
		if (reply)
			freeReplyObject(reply);
		LOG(ERROR) << "RedisCommand error=" << e.what();
		return false;
	}

	freeReplyObject(reply);
	return true;
}

bool RedisClient::Append(std::string& key, std::string& value) {
	redisReply* reply = static_cast<redisReply*>(redisCommand(redisContext_,
	                                                          "APPEND %s %s",
	                                                          key.c_str(),
	                                                          value.c_str()));
	if (!checkReply(reply)) {
		RedisException e(PREPARE_REDIS_EXCEPTION());
		if (reply)
			freeReplyObject(reply);
		LOG(ERROR) << "RedisCommand error=" << e.what();
		return false;
	}

	freeReplyObject(reply);
	return true;
}

bool RedisClient::Hset(std::string& key, std::string& field, std::string& value) {
	redisReply* reply = static_cast<redisReply*>(redisCommand(redisContext_,
	                                                          "HSET %s %s %s",
	                                                          key.c_str(),
	                                                          field.c_str(),
	                                                          value.c_str()));
	if (!checkReply(reply)) {
		RedisException e(PREPARE_REDIS_EXCEPTION());
		if (reply)
			freeReplyObject(reply);
		LOG(ERROR) << "RedisCommand error=" << e.what();
		return false;
	}

	freeReplyObject(reply);
	return reply->integer;
}
