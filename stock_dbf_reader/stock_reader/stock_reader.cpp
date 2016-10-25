// stock_reader.cpp : 定义控制台应用程序的入口点。
//

#include "stdafx.h"
#include "glog/logging.h"
#include "configure.h"
#include "dbf_reader.h"
#include "dbf.h"

#include <iostream>
#include <windows.h>


#define MAX_THREADS    10

using namespace dbf;

enum eProcessType {
	SH_DBF_PROCESS = 0,
	SZ_DBF_PROCESS = 1,
};

DbfReader* create_reader(int type);

int main(int argc, char** argv) {

	DeleteMenu(GetSystemMenu(GetConsoleWindow(), FALSE), SC_CLOSE, MF_BYCOMMAND);
	DrawMenuBar(GetConsoleWindow());

	// Init log module
	google::InitGoogleLogging(argv[0]);
	google::SetStderrLogging(google::GLOG_INFO);
	google::SetLogDestination(google::GLOG_INFO, "log/INFO_");
	google::SetLogDestination(google::GLOG_WARNING, "log/WARNING_");
	google::SetLogDestination(google::GLOG_ERROR, "log/ERROR_");

	// Load config
	Configure* config = Configure::GetPtr();
	if (NULL == config) {
		return -1;
	}

	// Create producer thread
	std::thread threads[2];
	for (int i = 0; i < 2; ++i) {
		DbfReader* producer = create_reader(i);
		threads[i] = producer->Start();
	}

	// Join all thread
	for (int i = 0; i < 2; ++i) {
		threads[i].join();
	}
	google::ShutdownGoogleLogging();
	return 0;
}

DbfReader* create_reader(int type) {
	// Load config, Check file path
	std::string dbfFilePath = Configure::GetRef().GetDbfPath();
	if (dbfFilePath.empty()) {
		MessageBox(NULL, TEXT("FilePath not exists"), TEXT("ERROR"), MB_OK);
		exit(-1);
	}
	if (dbfFilePath.find_last_of('\\') != dbfFilePath.size()) {
		dbfFilePath += "\\";
	}
	if (SH_DBF_PROCESS == type) {
		dbfFilePath += "SH.DBF";
	}
	else if (SZ_DBF_PROCESS == type) {
		dbfFilePath += "SZ.DBF";
	}
	return new DbfReader(dbfFilePath);
}
