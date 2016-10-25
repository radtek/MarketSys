//
// Created by harvey on 2016/9/14.
//

#include "stdafx.h"
#include "dbf.h"
#include <algorithm>
#include <math.h>
#include "lock.h"
#include "glog/logging.h"
#include "utils.hpp"

namespace dbf {

	static std::string ConvertNumber(uint8* n, int nSize) {
		// convert any size of number (represented by n[] ) into a string
		long long nResult = 0;
		for (int i = 0; i < nSize; i++) {
			nResult += (((unsigned long long) n[i]) << (i * 8));
		}
		std::stringstream ss; //create a stringstream
		ss << nResult; //add number to the stream
		return ss.str(); //return a string with the contents of the stream
	}

	Dbf::Dbf() {
		Init();
	}

	Dbf::~Dbf() {
		if (NULL != m_pFileHandler_) {
			fclose(m_pFileHandler_);
		}
		m_pFileHandler_ = NULL;
		delete[] m_pRecord_;
		m_sFileName_ = "";
		m_nNumFields_ = 0;
	}

	bool Dbf::Init() {
		m_pRecord_ = new char[MAX_RECORD_SIZE];
		memset(&m_FileHeader_, 0, sizeof(m_FileHeader_));
		return true;
	}

	bool Dbf::Open(const std::string file_name) {
		m_sFileName_ = file_name;
		m_pFileHandler_ = fopen(m_sFileName_.c_str(), "rb");
		if (NULL == m_pFileHandler_) {
			LOG(ERROR) << "Unable to open file: " << m_sFileName_ << ", errno: " << strerror(errno);
			return false;
		}
		// read dbf file header
		size_t nReadBytes = fread(&m_FileHeader_, 1, DBF_FILE_HEADER_SIZE, m_pFileHandler_);
		if (nReadBytes != DBF_FILE_HEADER_SIZE) {
			LOG(ERROR) << "Bad read for header, wanted 32, got : " << nReadBytes;
			return false;
		}
		m_nNumFields_ = 0;
		do {
			DbfFieldDefinition field_definition;
			nReadBytes = fread(&field_definition, 1, DBF_FIELD_HEADER_SIZE, m_pFileHandler_);
			if (nReadBytes != DBF_FIELD_HEADER_SIZE) {
				LOG(ERROR) << "Bad read for header, wanted 32, got : " << nReadBytes;
				return false;
			}
			if (field_definition.cFieldName[0] == HEAD_END_FALG) {
				//end of fields;
				break;
			}
			m_vecFieldDefinitions_.push_back(field_definition);
			++m_nNumFields_;
		} while (!feof(m_pFileHandler_));

		//求每个字段的偏移 偏移 = 上一个字段的偏移 + 上一个字段的长度 （第一个字段的偏移默认为1， 为删除位）
		int uFieldOffset = 1;
		FIELD_VEC::iterator iter(m_vecFieldDefinitions_.begin());
		for (; m_vecFieldDefinitions_.end() != iter; ++iter) {
			DbfFieldDefinition& field_definition = (*iter);
			field_definition.uFieldOffset = uFieldOffset;
			uFieldOffset += field_definition.uLength;
		}
		if (uFieldOffset != m_FileHeader_.uRecordLength) {
			LOG(ERROR) << "Bad Record length calculated from field sizes: "
				<< uFieldOffset << ", header says " << m_FileHeader_.uRecordLength;
			return false;
		}
		// move to start of first record   Head32 -> 32 * record_head -> end_head --> first
		int nFilePosForRec0 = 32 + 32 * m_nNumFields_ + 1;
		if (m_FileHeader_.u8FileType == 0x30) {
			//Visual   FoxPro 数据表文件 +264 还包含有.DBC文件路径的相关信息
			nFilePosForRec0 += 264;
		}
		if (m_FileHeader_.uPositionOfFirstRecord != nFilePosForRec0) {
			LOG(ERROR) << "Bad Record length calculated from field sizes: "
				<< nFilePosForRec0 << ", header says " << m_FileHeader_.uPositionOfFirstRecord;
			return false;
		}
		return true;
	}

	bool Dbf::LoadRecord(int nRecord) {
		int nPos = m_FileHeader_.uPositionOfFirstRecord + m_FileHeader_.uRecordLength * nRecord;
		int nRes = fseek(m_pFileHandler_, nPos, SEEK_SET);
		if (0 != nRes) {
			//		std::cerr << __FUNCTION__ << " Error seeking to record " << nRecord << " at " << nPos << " err="
			//			<< ferror(m_pFileHandler_) << std::endl;
			for (unsigned int i = 0; i < sizeof(m_pRecord_); i++)
				m_pRecord_[i] = 0; // clear record to indicate it is invalid
			return false;
		}

		size_t nBytesRead = fread(&m_pRecord_[0], 1, m_FileHeader_.uRecordLength, m_pFileHandler_);
		if (m_FileHeader_.uRecordLength != nBytesRead) {
			//		std::cerr << __FUNCTION__ << " read(" << nRecord << ") failed, wanted " << m_FileHeader_.uRecordLength
			//			<< ", but got " << nBytesRead << " bytes";
			for (unsigned int i = 0; i < sizeof(m_pRecord_); i++)
				m_pRecord_[i] = 0; // clear record to indicate it is invalid
			return false; //fail
		}
		return true;
	}

	std::string Dbf::ReadFieldAsString(int nField) {

		if (nField >= m_vecFieldDefinitions_.size()) {
			std::cerr << __FUNCTION__ << " ReadFieldAsString nfield out of index" << std::endl;
			return "";
		}

		// read the field from record
		char cType = m_vecFieldDefinitions_[nField].cFieldType;
		int nOffset = m_vecFieldDefinitions_[nField].uFieldOffset;
		int nMaxSize = m_vecFieldDefinitions_[nField].uLength;

		/* 字段类型
		C=char
		Y=Currency (? format unknown, probably char)
		N=Numeric (really just a char)
		F=Float (really just a char)
		D=Date (? format unknown, probably char)
		T=Date Time (? format unknown, probably char)
		B=Double (an actual double)
		I=Integer (4 byte int)
		L=Logical (char[1] with T=true,?=Null,F=False)
		M=memo (big char field?)
		G=General (?)
		P=Picture (?)
		... others?  really no idea
		treat all unhandled field types as C for now
		*/
		if ('I' == cType) {
			uint8 n[16];
			for (int i = 0; i < nMaxSize; ++i) {
				n[i] = (uint8)m_pRecord_[nOffset + i];
			}
			return ConvertNumber(&n[0], nMaxSize);
		}
		else if ('B' == cType) {
			if (4 == nMaxSize) {
				//float
				union name1 {
					float f;
					uint8 n[4];
				} uvar;

				uvar.f = 0.0;
				uvar.n[0] = (uint8)m_pRecord_[nOffset];
				uvar.n[1] = (uint8)m_pRecord_[nOffset + 1];
				uvar.n[2] = (uint8)m_pRecord_[nOffset + 2];
				uvar.n[3] = (uint8)m_pRecord_[nOffset + 3];

				std::stringstream ss;
				ss.precision(8);
				ss << uvar.f;
				return ss.str();
			}
			else if (8 == nMaxSize) {
				//double
				union name1 {
					double d;
					uint8 n[8];
				} uvar;

				uvar.d = 0.0;
				uvar.n[0] = (uint8)m_pRecord_[nOffset];
				uvar.n[1] = (uint8)m_pRecord_[nOffset + 1];
				uvar.n[2] = (uint8)m_pRecord_[nOffset + 2];
				uvar.n[3] = (uint8)m_pRecord_[nOffset + 3];
				uvar.n[4] = (uint8)m_pRecord_[nOffset + 4];
				uvar.n[5] = (uint8)m_pRecord_[nOffset + 5];
				uvar.n[6] = (uint8)m_pRecord_[nOffset + 6];
				uvar.n[7] = (uint8)m_pRecord_[nOffset + 7];

				std::stringstream ss;
				ss.precision(17);
				ss << uvar.d;
				return ss.str();
			}
		}
		else if ('L' == cType) {
			// Logical ,T = true, ?=NULL, F=False
			if (strncmp(&(m_pRecord_[nOffset]), "T", 1) == 0)
				return "T";
			else if (strncmp(&(m_pRecord_[nOffset]), "?", 1) == 0)
				return "?";
			else
				return "F";
		}
		else {
			// Character type fields (default)
			char dest[256]; // Fields can not exceed 255 chars
			nMaxSize = min(nMaxSize, 256);
			memset(&dest, 0, strlen(dest));
			//        for (int i = 0; i < nMaxSize; i++)
			//            dest[i] = 0; // clear past end of usable string in case it is missing a terminator
			strncpy(&dest[0], &m_pRecord_[nOffset], nMaxSize);
			std::stringstream ss;
			ss << dest;
			return ss.str();
		}

		return "FAIL";
	}

	double Dbf::ReadFieldAsDouble(int nField) {
		// read the request field as a double to get higher performance for 'B' type fields only!
		char cType = m_vecFieldDefinitions_[nField].cFieldType;
		int nOffset = m_vecFieldDefinitions_[nField].uFieldOffset;
		int nMaxSize = m_vecFieldDefinitions_[nField].uLength;

		if (cType == 'B') {
			// handle real float or double
			if (nMaxSize == 4) {
				// float
				union name1 {
					uint8 n[4];
					float f;
				} uvar;
				uvar.f = 0;

				uvar.n[0] = (uint8)m_pRecord_[nOffset];
				uvar.n[1] = (uint8)m_pRecord_[nOffset + 1];
				uvar.n[2] = (uint8)m_pRecord_[nOffset + 2];
				uvar.n[3] = (uint8)m_pRecord_[nOffset + 3];

				return uvar.f;
			}
			else if (nMaxSize == 8) {
				// double
				union name1 {
					uint8 n[8];
					double d;
				} uvar;
				uvar.d = 0;

				uvar.n[0] = (uint8)m_pRecord_[nOffset];
				uvar.n[1] = (uint8)m_pRecord_[nOffset + 1];
				uvar.n[2] = (uint8)m_pRecord_[nOffset + 2];
				uvar.n[3] = (uint8)m_pRecord_[nOffset + 3];
				uvar.n[4] = (uint8)m_pRecord_[nOffset + 4];
				uvar.n[5] = (uint8)m_pRecord_[nOffset + 5];
				uvar.n[6] = (uint8)m_pRecord_[nOffset + 6];
				uvar.n[7] = (uint8)m_pRecord_[nOffset + 7];

				return uvar.d;
			}
		}
		return -9e99; // fail !!!
	}

	bool Dbf::HandleDbfRecords(std::string& redis_command,
		const std::map<std::string, bool>& valid_stock_map,
		std::string& last_load_time) {
		std::string update_day;
		std::string update_time;
		std::stringstream all_redis_ss;
		int total_read = 0;
		for (int i = 0; i < GetNumRecords(); ++i) {
			if (!LoadRecord(i)) {
				return false;
			}
			bool record_valid = true;
			std::stringstream redis_ss;

			for (int f = 0; f < m_nNumFields_; ++f) {
				DbfFieldDefinition& dbfFieldDefinition = m_vecFieldDefinitions_.at(f);
				std::string field_name = dbfFieldDefinition.cFieldName;
				std::string value = ReadFieldAsString(f);
				base_logic::Utils::trim(value);

				//第一条获取本次数据的更新日期和时间
				if (0 == i) {
					if ("NAME" == field_name)
						update_time = value;
					if ("HIGH" == field_name) {
						update_day = value;
						if(!CheckNewUpdate(update_day, update_time, last_load_time)) {
							return false;
						}
						else {
							record_valid = false;
							break;
						}
					}
					continue;
				}
				// Check code valid
				if ("CODE" == field_name) {
					// Check like hs300 etc...
					if (!CheckSpecialStock(value)) {
						// Check is exchange_type stock
						if (valid_stock_map.end() == valid_stock_map.find(value)) {
							record_valid = false;
							break;
						}
					}
				}
	
				GenerateRedisCommand(redis_ss,
					dbfFieldDefinition.cFieldType,
					field_name,
					value);
			}
			if (record_valid) {
				all_redis_ss << redis_ss.str() << ";";
				++total_read;
			}
		}
		LOG(INFO) << m_sFileName_ 
			<< " --> " 
			<< total_read 
			<< " records, update_time " 
			<< update_time;
		redis_command = all_redis_ss.str();
		return true;
	}

	inline void Dbf::GenerateRedisCommand(std::stringstream& ss,
		char field_type,
		std::string& fieldname,
		std::string& fieldvalue) {

		// 股票名称
		if ("NAME" != fieldname) {
			// redis
			if (field_type == 'B' && atof(fieldvalue.c_str()) != 0.0) {
				ss << fieldvalue;
			}
			else {
				ss << fieldvalue;
			}
		}
		ss << ",";

	}

	inline bool Dbf::CheckSpecialStock(std::string& stock_code) {
		static const std::string szzs = "000001";
		static const std::string szcz = "399001";
		static const std::string cybz = "399006";
		static const std::string hs300 = "000300";

		if (szzs == stock_code || szcz == stock_code || cybz == stock_code || hs300 == stock_code) {
			return true;
		}
		return false;
	}

	inline bool Dbf::CheckNewUpdate(const std::string& update_day, 
							 const std::string& update_time,
							 std::string& last_load_time) {
		std::string current_day = base_logic::Utils::GetTimeOfDay();
		if (!update_day.empty() && update_day == current_day) {
			if (!update_time.empty() && update_time != last_load_time) {
				last_load_time = update_time;
				return true;
			}
		}
		return false;
	}


} /*namespace dbf*/
