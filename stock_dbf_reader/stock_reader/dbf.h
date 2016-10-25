//
// Created by harvey on 2016/9/14.
//

#ifndef DBF_READER_DBF_H
#define DBF_READER_DBF_H

#include <iostream>
#include <memory>
#include <fcntl.h>
#include <errno.h>
#include <map>
#include <list>
#include <vector>
#include <sstream>
#include <stdio.h>

typedef unsigned char uint8;
typedef short int uint16;
typedef int uint32;

#define DBF_FILE_HEADER_SIZE			32
#define DBF_FIELD_HEADER_SIZE			32
#define HEAD_END_FALG					(0x0D)
#define MAX_RECORD_SIZE                 0xffff*50

namespace dbf {

namespace {

//DBF文件头的详细格式
struct DbfFileHeader {
	uint8 u8FileType;
	uint8 u8LastUpdateYear;
	uint8 u8LastUpdateMonth;
	uint8 u8LastUpdateDay;
	uint32 uRecordsInFile;
	uint16 uPositionOfFirstRecord;
	uint16 uRecordLength; // includes the delete flag (byte) at start of record
	uint8 Reserved16[4 * 4]; // 16 bytes reserved, must always be set to zeros
	uint8 uTableFlags;
	uint8 uCodePage;
	uint8 Reserved2[2]; // 2 bytes reserved, must put zeros in all reserved fields
};

struct DbfFieldDefinition {
	char cFieldName[11];
	char cFieldType;
	uint32 uFieldOffset; // location of field from start of record
	uint8 uLength; // Length of Field in bytes
	uint8 uNumberOfDecimalPlaces;
	uint8 FieldFlags;
	uint8 uNextAutoIncrementValue[4]; // 32 bit int
	uint8 uAutoIncrementStep;
	uint8 Reserved8[8]; // should always be zero
};

} /*namespace*/


typedef std::vector<DbfFieldDefinition> FIELD_VEC;

class Dbf {
public:
	Dbf();
	~Dbf();
	bool Init();
	bool Open(const std::string file_name);

private:
	bool LoadRecord(int record);
	std::string ReadFieldAsString(int nField);
	double ReadFieldAsDouble(int field);

public:
	bool HandleDbfRecords(std::string& redis_command,
						  const std::map<std::string, bool>& valid_stock_map,
						  std::string& last_load_time);

	int GetNumRecords() const {
		return m_FileHeader_.uRecordsInFile;
	}

	int GetNumFields() const {
		return m_nNumFields_;
	}

private:
	inline void GenerateRedisCommand(std::stringstream& ss,
	                                 char field_type,
	                                 std::string& fieldname,
	                                 std::string& fieldvalue);
	inline bool CheckSpecialStock(std::string& stock_code);
	inline bool CheckNewUpdate(const std::string& update_day, 
							   const std::string& update_time,
							   std::string& last_load_time);

private:
	FILE* m_pFileHandler_;
	std::string m_sFileName_;
	DbfFileHeader m_FileHeader_;
	FIELD_VEC m_vecFieldDefinitions_;
	int m_nNumFields_;
	char* m_pRecord_;
};

using DbfPtr = std::shared_ptr<Dbf>;

} /*namespace dbf*/

#endif //DBF_READER_DBF_H
