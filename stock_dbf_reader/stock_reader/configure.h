#pragma once

#include "common.h"

enum eTagType {
	TG_VALID,
	TG_SELF,
	TG_REDIS,
	TG_MYSQL,
};


class Configure {
public:
	~Configure();
	static Configure* GetPtr();
	static Configure& GetRef();

	bool LoadConfig();

private:
	Configure();
	Configure(const Configure& rhs) = default;
	Configure& operator=(const Configure& rhs) = default;

private:
	bool GetLineTag(const std::string& line, eTagType& tag);
	bool ParseLine(std::string& line);

	bool ParseSelf(std::string& line, std::string& value);
	bool ParseConn(base_logic::ConnAddr& conn_add, std::string& line, std::string& value);

public:
	std::string GetDbfPath() const {
		return m_sDbfPath_;
	}

	const base_logic::ConnAddr& GetMySqlInfo() const {
		return m_mysqlInfo_;
	}

	const base_logic::ConnAddr& GetRedisInfo() const {
		return m_redisInfo_;
	}

	int GetSZDBFWait() const {
		return m_iSzDbfParseInterVal_;
	}

	int GetSHDBFWait() const {
		return m_iShDbfParseInterval_;
	}

	int GetThreadPoolNum() const {
		return thread_pool_num_;
	}

private:
	base_logic::ConnAddr m_mysqlInfo_;
	base_logic::ConnAddr m_redisInfo_;
	std::string m_sDbfPath_;
	int m_iShDbfParseInterval_;
	int m_iSzDbfParseInterVal_;
	int thread_pool_num_;
	eTagType m_eTagType_;
};



