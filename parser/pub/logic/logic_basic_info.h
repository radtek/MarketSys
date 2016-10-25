/*
 * logic_basic_info.h
 * baby
 *
 *  Created on: 2015年7月28日
 *      Author: kerry
 */

#ifndef PUB_LOGIC_BASIC_INFO_H_
#define PUB_LOGIC_BASIC_INFO_H_

#include "logic/base_values.h"
#include "basic/scoped_ptr.h"
#include "basic/basictypes.h"
#include <string>

namespace base_logic{

typedef scoped_ptr<base_logic::ListValue>  InfoAttrType;

class NewsUnit{
public:
	explicit NewsUnit();

	NewsUnit(const NewsUnit& news);
	NewsUnit& operator = (const NewsUnit& news);

	base_logic::DictionaryValue* Release();

	const void set_id(const int64 id){data_->id_ = id;}
	const void set_cmt_count(const int64 cmt_count){data_->cmt_count_ = cmt_count;}
	const void set_is_original(const bool original){data_->is_original_ = original;}
	const void set_type(const int32 type){data_->type_ = type;}
	const void set_publish_time(const int64 publish_time){data_->publish_time_ = publish_time;}
	const void set_title(const std::string& title){data_->title_ = title;}
	const void set_content(const std::string& content){data_->content_ = content;}
	const void set_img_url(const std::string& img_url){
		data_->img_url_->Append(base_logic::Value::CreateStringValue(img_url));
	}


	const int64 id() const {return data_->id_;}
private:
	class Data{
	public:
		Data()
		:refcount_(1)
		,id_(0)
		,type_(0)
		,cmt_count_(0)
		,is_original_(false)
		,publish_time_(time(NULL)){
			img_url_.reset(new base_logic::ListValue());
		}

	public:
		int64         id_;
		int32         type_;
		int64         publish_time_;
		int64         cmt_count_;
		bool          is_original_;
		std::string   title_;
		std::string   content_;
		InfoAttrType  img_url_;
		void AddRef(){refcount_ ++;}
		void Release(){if (!--refcount_)delete this;}
	private:
		int refcount_;

	};

	Data*             data_;
};


class CourseUnit{
public:
	explicit CourseUnit();

	CourseUnit(const CourseUnit& course);
	CourseUnit& operator = (const CourseUnit& course);

	base_logic::DictionaryValue* Release();

	const void set_id(const int64 id){data_->id_ = id;}
	const void set_play_count(const int64 play_count){data_->play_count_ = play_count;}
	const void set_cmt_count(const int64 cmt_count){data_->cmt_count_ = cmt_count;}
	const void set_type(const int64 type){data_->type_ = type;}
	const void set_title(const std::string& title){data_->title_ = title;}
	const void set_description(const std::string& description){data_->description_ = description;}
	const void set_imgurl(const std::string& img_url){data_->img_url_ = img_url;}
	const void set_attr(const int32 attr){data_->attr_ = attr;}


	const int32 attr() const {return data_->attr_;}
private:
	class Data{
	public:
		Data()
		:id_(0)
		,refcount_(1)
		,play_count_(0)
		,cmt_count_(0)
		,type_(0)
		,attr_(0){}
	public:
		int64        id_;
		int64        play_count_;
		int64        cmt_count_;
		int32        type_;
		int32        attr_;
		std::string  title_;
		std::string  description_;
		std::string  img_url_;
		void AddRef(){refcount_ ++;}
		void Release(){if (!--refcount_)delete this;}
	private:
		int refcount_;
	};
	Data*             data_;
};

class LBSInfos{
public:
	explicit LBSInfos();
	explicit LBSInfos(const std::string& host,const double& latitude,
			const double& longitude,const std::string& city,const std::string& district,
			const std::string& province,std::string& street);

	LBSInfos(const LBSInfos& lbs_basic_info);
	LBSInfos& operator = (const LBSInfos& lbs_basic_info);

	const std::string& host() const {return data_->host_;}
	const double latitude() const {return data_->latitude_;}
	const double longitude() const {return data_->longitude_;}
	const std::string& city() const {return data_->city_;}
	const std::string& district() const {return data_->district_;}
	const std::string& province() const {return data_->province_;}
	const std::string& street() const {return data_->street_;}


private:
	class Data{
	public:
		Data():refcount_(1)
		,latitude_(0.0)
		,longitude_(0.0){}

		Data(const std::string& host,const double latitude,
				const double longitude,const std::string& city,const std::string& district,
				const std::string& province,std::string& street)
		:refcount_(1)
		,latitude_(latitude)
		,longitude_(longitude)
		,city_(city)
		,district_(district)
		,province_(province)
		,street_(street){}
		void AddRef(){refcount_ ++;}
		void Release(){if (!--refcount_)delete this;}
	public:
		const std::string host_;
		const double latitude_;
		const double longitude_;
		const std::string city_;
		const std::string district_;
		const std::string province_;
		const std::string street_;
	private:
		int refcount_;
	};
	Data*       data_;
};

}
#endif
