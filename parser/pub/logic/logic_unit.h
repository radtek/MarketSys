/*
 * logic_unit.h
 *
 *  Created on: 2014年12月2日
 *      Author: kerry
 */
#ifndef _BABY_LOGIC_UNIT_H__
#define _BABY_LOGIC_UNIT_H__

#include <string>
#include "logic/logic_basic_info.h"
#include "lbs/lbs_connector.h"
#include "logic/logic_comm.h"
#include "net/comm_head.h"
#include "net/http_data.h"
namespace base_logic{

class LogicUnit{
public:

	static void SendMessage(const int socket,SendPacketBase* packet, int ntype = 0);


	static void SendErrorMsg(const int32 error_code,const int socket, int ntype = 0, std::string jsonp="");

	static void CreateToken(const int64 uid,std::string& token);

};
}

#define send_error      base_logic::LogicUnit::SendErrorMsg
#define send_message    base_logic::LogicUnit::SendMessage

#endif

