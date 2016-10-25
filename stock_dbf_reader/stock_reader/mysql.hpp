#ifndef MYSQL_HPP_
#define MYSQL_HPP_

#include <cassert>
#include <cstdint>
#include <cstring>
#include <mysql.h>

#include <string>
#include <tuple>
#include <typeinfo>
#include <utility>
#include <vector>

#include "inpub_binder.hpp"
#include "mysql_exception.hpp"
#include "mysql_prepared_statement.hpp"
#include "output_binder.hpp"
#include "common.h"
#include "utils.hpp"

class mysql {
public:
	mysql(base_logic::ConnAddr& configInfo);
	~mysql();

	mysql(const mysql& rhs) = delete;
	mysql(mysql&& rhs) = delete;
	mysql& operator=(const mysql& rhs) = delete;
	mysql& operator=(mysql&& rhs) = delete;

public:
	void OnDisconnec();
	bool ConnectMySql();
	bool Reconnect();
	bool CheckConnected();

	/**
	 * Normal query. Results are stored in the given vector.
	 * @param query The query to run.
	 * @param results A vector of tuples to store the results in.
	 * @param args Arguments to bind to the query.
	 */
	template <typename... InputArgs, typename... OutputArgs>
	void runQuery(
		std::vector<std::tuple<OutputArgs...>>* const results,
		const char* const query,
		// Args needs to be sent by reference, because the values need to be
		// nontemporary (that is, lvalues) so that their memory locations
		// can be bound to MySQL's prepared statement API
		const InputArgs& ... args);

	/**
	 * Command that doesn't return results, like "USE yelp" or
	 * "INSERT INTO user VALUES ('Brandon', 28)".
	 * @param query The query to run.
	 * @param args Arguments to bind to the query.
	 * @return The number of affected rows.
	 */
	/// @{
	template <typename... Args>
	my_ulonglong runCommand(
		const char* const command,
		// Args needs to be sent by reference, because the values need to be
		// nontemporary (that is, lvalues) so that their memory locations
		// can be bound to MySQL's prepared statement API
		const Args& ... args) const;
	my_ulonglong runCommand(const char* const command);
	/// @}

	/**
	 * Prepare a statement for multiple executions with different bound
	 * parameters. If you're running a one off query or statement, you
	 * should use runQuery or runCommand instead.
	 * @param query The query to prepare.
	 * @return A prepared statement object.
	 */
	MySqlPreparedStatement prepareStatement(const char* statement) const;

	/**
	 * Run the command version of a prepared statement.
	 */
	/// @{
	template <typename... Args>
	my_ulonglong runCommand(
		const MySqlPreparedStatement& statement,
		const Args& ... args) const;
	//    my_ulonglong runCommand(const MySqlPreparedStatement& statement);
	/// @}

	/**
	 * Run the query version of a prepared statement.
	 */
	template <typename... InputArgs, typename... OutputArgs>
	void runQuery(
		std::vector<std::tuple<OutputArgs...>>* results,
		const MySqlPreparedStatement& statement,
		const InputArgs& ...) const;

private:
	base_logic::ConnAddr& m_configInfo_;
	int m_iReconnectCount_ = 0;
	MYSQL* m_pConnection_;
};

template <typename... Args>
my_ulonglong mysql::runCommand(
	const char* const command,
	const Args& ... args
) const {
	MySqlPreparedStatement statement(prepareStatement(command));
	return runCommand(statement, args...);
}

template <typename... Args>
my_ulonglong mysql::runCommand(
	const MySqlPreparedStatement& statement,
	const Args& ... args
) const {
	// Commands (e.g. INSERTs or DELETEs) should always have this set to 0
	if (0 != statement.getFieldCount()) {
		throw MySqlException("Tried to run query with runCommand");
	}

	if (sizeof...(args) != statement.getParameterCount()) {
		std::string errorMessage;
		errorMessage += "Incorrect number of parameters; command required ";
		errorMessage += base_logic::Utils::convertString(statement.getParameterCount());
		errorMessage += " but ";
		errorMessage += base_logic::Utils::convertString(sizeof...(args));
		errorMessage += " parameters were provided.";
		throw MySqlException(errorMessage);
	}

	std::vector<MYSQL_BIND> bindParameters;
	bindParameters.resize(statement.getParameterCount());
	bindInputs<Args...>(&bindParameters, args...);
	if (0 != mysql_stmt_bind_param(
			statement.statementHandle_,
			bindParameters.data())
	) {
		throw MySqlException(statement);
	}

	if (0 != mysql_stmt_execute(statement.statementHandle_)) {
		throw MySqlException(statement);
	}

	// If the user ran a SELECT statement or something else, at least warn them
	const auto affectedRows = mysql_stmt_affected_rows(
		statement.statementHandle_);
	if ((static_cast<decltype(affectedRows)>(-1)) == affectedRows) {
		throw MySqlException("Tried to run query with runCommand");
	}

	return affectedRows;
}

template <typename... InputArgs, typename... OutputArgs>
void mysql::runQuery(
	std::vector<std::tuple<OutputArgs...>>* const results,
	const char* const query,
	const InputArgs& ... args) {
	assert(nullptr != results);
	assert(nullptr != query);
	MySqlPreparedStatement statement(prepareStatement(query));
	runQuery(results, statement, args...);
}

template <typename... InputArgs, typename... OutputArgs>
void mysql::runQuery(
	std::vector<std::tuple<OutputArgs...>>* const results,
	const MySqlPreparedStatement& statement,
	const InputArgs& ... args
) const {
	assert(nullptr != results);
	// SELECTs should always return something. Commands (e.g. INSERTs or
	// DELETEs) should always have this set to 0.
	if (0 == statement.getFieldCount()) {
		throw MySqlException("Tried to run command with runQuery");
	}

	// Bind the input parameters
	// Check that the parameter count is right
	if (sizeof...(InputArgs) != statement.getParameterCount()) {
		std::string errorMessage;

		errorMessage += "Incorrect number of input parameters; query required ";
		errorMessage += base_logic::Utils::convertString(statement.getParameterCount());
		errorMessage += " but ";
		errorMessage += base_logic::Utils::convertString(sizeof...(args));
		errorMessage += " parameters were provided.";
		throw MySqlException(errorMessage);
	}

	std::vector<MYSQL_BIND> inputBindParameters;
	inputBindParameters.resize(statement.getParameterCount());
	bindInputs<InputArgs...>(&inputBindParameters, args...);
	if (0 != mysql_stmt_bind_param(
			statement.statementHandle_,
			inputBindParameters.data())
	) {
		throw MySqlException(statement);
	}

	setResults<OutputArgs...>(statement, results);
}

#endif  // MYSQL_HPP_
