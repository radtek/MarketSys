#include "stdafx.h"

#include "configure.h"
#include "utils.hpp"

#include <windows.h>

Configure::Configure()
	: m_eTagType_(TG_VALID)
	  , m_iSzDbfParseInterVal_(2)
	  , m_iShDbfParseInterval_(2)
	  , thread_pool_num_(2) {
	memset(&m_mysqlInfo_, 0, sizeof(m_mysqlInfo_));
	memset(&m_redisInfo_, 0, sizeof(m_redisInfo_));
	if(!LoadConfig()) {
		exit(-1);
	}
}

Configure::~Configure() {

}

Configure* Configure::GetPtr() {
	return &GetRef();
}

Configure& Configure::GetRef() {
	static Configure instance;
	return instance;
}

bool Configure::LoadConfig() {
	std::ifstream ifs("./config.ini", std::ios::in);
	if (!ifs.is_open()) {
		MessageBox(NULL, TEXT("Configure open file error"), TEXT("ERROR"), MB_OK);
		return false;
	}
	while (!ifs.eof()) {
		std::string line;
		std::getline(ifs, line);
		base_logic::Utils::repaceTab(line);
		ParseLine(line);
	}
	return true;
}

bool Configure::GetLineTag(const std::string& line, eTagType& tag) {
	size_t nPos;
	nPos = line.find("self");
	if (std::string::npos != nPos) {
		tag = TG_SELF;
		return true;
	}
	nPos = line.find("mysql");
	if (std::string::npos != nPos) {
		tag = TG_MYSQL;
		return true;
	}
	nPos = line.find("redis");
	if (std::string::npos != nPos) {
		tag = TG_REDIS;
		return true;
	}
	tag = m_eTagType_;
	return TG_VALID != tag;
}

bool Configure::ParseLine(std::string& line) {
	eTagType type;
	if (!GetLineTag(line, type)) {
		return false;
	}
	if (line.empty()) {
		return false;
	}
	std::string value = line.substr(line.find_first_of(" ") + 1);
	base_logic::Utils::trim(value);
	switch (type) {
	case TG_SELF: {
		ParseSelf(line, value);
		break;
	}
	case TG_MYSQL: {
		ParseConn(m_mysqlInfo_, line, value);
		break;
	}
	case TG_REDIS: {
		ParseConn(m_redisInfo_, line, value);
		break;
	}
	default:
		return false;
	}
	m_eTagType_ = type;
	return true;
}

bool Configure::ParseSelf(std::string& line, std::string& value) {
	size_t nPos;
	nPos = line.find("dbf_path");
	if (std::string::npos != nPos) {
		m_sDbfPath_ = value;
		return true;
	}
	nPos = line.find("SH.DBF_interval");
	if (std::string::npos != nPos) {
		m_iShDbfParseInterval_ = atoi(value.c_str());
	}
	nPos = line.find("SZ.DBF_interval");
	if (std::string::npos != nPos) {
		m_iSzDbfParseInterVal_ = atoi(value.c_str());
	}
	nPos = line.find("consumer_num");
	if (std::string::npos != nPos) {
		thread_pool_num_ = atoi(value.c_str());
	}
	return true;
}

bool Configure::ParseConn(base_logic::ConnAddr& conn_add, std::string& line, std::string& value) {
	size_t nPos;
	nPos = line.find("address");
	if (std::string::npos != nPos) {
		conn_add.st_address = value;
	}
	nPos = line.find("port");
	if (std::string::npos != nPos) {
		conn_add.st_port = value;
	}
	nPos = line.find("username");
	if (std::string::npos != nPos) {
		conn_add.st_username = value;
	}
	nPos = line.find("pass");
	if (std::string::npos != nPos) {
		conn_add.st_password = value;
	}
	nPos = line.find("dbname");
	if (std::string::npos != nPos) {
		conn_add.st_dbname = value;
	}
	return true;
}
