#include "stdafx.h"
#include "mysql.hpp"
#include "mysql_exception.hpp"

#include "glog/logging.h"
#include "common.h"

using std::string;
using std::vector;

mysql::mysql(base_logic::ConnAddr& configInfo)
        : m_configInfo_(configInfo){
    ConnectMySql();
}

mysql::~mysql() {
    mysql_close(m_pConnection_);
    m_pConnection_ = NULL;
}

bool mysql::ConnectMySql() {
    m_pConnection_ = mysql_init(nullptr);
    if (nullptr == m_pConnection_) {
        throw MySqlException("Unable to connect to mysql");
    }
	mysql_options(m_pConnection_, MYSQL_SET_CHARSET_NAME, "utf8");
    const MYSQL* const success = mysql_real_connect(
        m_pConnection_,
        m_configInfo_.st_address.c_str(),
        m_configInfo_.st_username.c_str(),
        m_configInfo_.st_password.c_str(),
        m_configInfo_.st_dbname.c_str(),
        3306,
        nullptr,
        0
    );
    if (nullptr == success) {
        MySqlException mes(m_pConnection_);
        mysql_close(m_pConnection_);
        throw mes;
    }
    std::string charter = "gb2312";
    mysql_set_character_set(m_pConnection_, charter.c_str());
	mysql_set_server_option(m_pConnection_, MYSQL_OPTION_MULTI_STATEMENTS_ON);
    return true;
}

void mysql::OnDisconnec() {
    if (m_pConnection_) {
        mysql_close(m_pConnection_);
    }
    m_pConnection_ = NULL;
}

bool mysql::CheckConnected() {
    if (nullptr == m_pConnection_) {
        return false;
    }
    int ret = mysql_ping(m_pConnection_);
	if( 0 != ret) {
		LOG(WARNING) << "mysql_ping error, code = " << ret;
	}
	return 0 == mysql_ping(m_pConnection_);
}

bool mysql::Reconnect() {
    OnDisconnec();
    return ConnectMySql();
}

my_ulonglong mysql::runCommand(const char* const command) {
    if (0 != mysql_real_query(m_pConnection_, command, strlen(command))) {
//        throw MySqlException(m_pConnection_);
        return 0;
    }

    // If the user ran a SELECT statement or something else, at least warn them
    const my_ulonglong affectedRows = mysql_affected_rows(m_pConnection_);
    if ((my_ulonglong) -1 == affectedRows) {
        // Clean up after the query
        MYSQL_RES* const result = mysql_store_result(m_pConnection_);
        mysql_free_result(result);

        throw MySqlException("Tried to run query with runCommand");
    }

    return affectedRows;
}

MySqlPreparedStatement mysql::prepareStatement(const char* const command) const {
    return MySqlPreparedStatement(command, m_pConnection_);
}
