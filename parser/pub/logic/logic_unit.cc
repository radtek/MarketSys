/*
 * logic_unit.cc
 *
 *  Created on: 2014年12月2日
 *      Author: kerry
 */
#include "logic/logic_unit.h"
#include "net/comm_head.h"
#include "net/error_comm.h"
#include "basic/scoped_ptr.h"
#include "basic/basic_util.h"
#include "http/http_method.h"
#include "basic/md5sum.h"
#include "basic/radom_in.h"
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <sstream>
namespace base_logic{

void LogicUnit::SendMessage(const int socket,SendPacketBase* packet, int ntype){
	std::string json;
	packet->GetJsonSerialize(&json, ntype);
	LOG_DEBUG2("%s",json.c_str());
	base_logic::LogicComm::SendFull(socket,json.c_str(),json.length());
}

void LogicUnit::SendErrorMsg(const int32 error_code,const int socket, int ntype, std::string jsonp){
	scoped_ptr<SendPacketBase> packet(new SendPacketBase());
	std::string error_msg = buddha_strerror(error_code);
	packet->set_flag(error_code);
	packet->set_msg(error_msg);
	packet->set_status(0);
  packet->set_jsonp_callback(jsonp);
	SendMessage(socket,packet.get(), ntype);
}

void LogicUnit::CreateToken(const int64 uid,std::string& token){
	std::stringstream os;
	os<<uid;
	//create token
	int32 random_num = base::SysRadom::GetInstance()->GetRandomID();
	//md5
	token="miglab";
	std::string key;
	os<<random_num;
	base::MD5Sum md5(os.str());
	token = md5.GetHash();
}

}
