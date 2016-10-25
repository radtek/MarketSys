#include "logic_basic_info.h"
#include <string>

namespace base_logic{

NewsUnit::NewsUnit(){
	data_ = new Data();
}


NewsUnit::NewsUnit(const NewsUnit& news)
:data_(news.data_){

}

NewsUnit& NewsUnit::operator =(const NewsUnit& news){
	if(news.data_!=NULL){
		news.data_->AddRef();
	}
	if(data_!=NULL){
		data_->Release();
	}
	data_ = news.data_;
	return (*this);
}

base_logic::DictionaryValue* NewsUnit::Release(){
	scoped_ptr<base_logic::DictionaryValue> dict(new base_logic::DictionaryValue());
	if(data_->id_!=0)
		dict->SetBigInteger(L"id",data_->id_);
	//if(data_->cmt_count_!=0)
	dict->SetBigInteger(L"cmt_count",data_->cmt_count_);
	//if()
	dict->SetBigInteger(L"publish_time",data_->publish_time_);
	dict->SetBoolean(L"is_original",data_->is_original_);
	dict->SetInteger(L"type",data_->type_);
	if(!data_->content_.empty())
		dict->SetString(L"content",data_->content_);
	if(!data_->title_.empty())
		dict->SetString(L"title",data_->title_);

	//图片集
	//scoped_ptr<base_logic::ListValue> list(new base_logic::ListValue());
	if(!data_->img_url_->empty())
		dict->Set(L"img_url",(base_logic::Value*)(data_->img_url_.get()));

	return dict.release();
}

CourseUnit::CourseUnit(){
	data_ = new Data();
}

CourseUnit::CourseUnit(const CourseUnit& course)
:data_(course.data_){

}

CourseUnit& CourseUnit::operator =(const CourseUnit& course){
	if(course.data_!=NULL){
		course.data_->AddRef();
	}

	if(data_!=NULL){
		data_->Release();
	}

	data_ = course.data_;
	return (*this);
}

base_logic::DictionaryValue* CourseUnit::Release(){
	scoped_ptr<base_logic::DictionaryValue> dict(new base_logic::DictionaryValue());
	if(data_->id_!=0)
		dict->SetBigInteger(L"id",data_->id_);
	dict->SetBigInteger(L"play_count",data_->play_count_);
	dict->SetBigInteger(L"cmt_count",data_->cmt_count_);
	if(data_->type_!=0)
		dict->SetInteger(L"type",data_->type_);

	if(!data_->title_.empty())
		dict->SetString(L"title",data_->title_);
	if(!data_->description_.empty())
		dict->SetString(L"desc",data_->description_);
	if(!data_->img_url_.empty())
		dict->SetString(L"imgurl",data_->img_url_);

	return dict.release();
}

LBSInfos::LBSInfos(){
	data_ = new Data();
}

LBSInfos::LBSInfos(const std::string& host,const double& latitude,
			const double& longitude,const std::string& city,const std::string& district,
			const std::string& province,std::string& street){
	data_ = new Data(host,latitude,longitude,city,district,province,street);
}

LBSInfos::LBSInfos(const LBSInfos& lbs_basic_info)
:data_(lbs_basic_info.data_){
	if(data_!=NULL){
		data_->AddRef();
	}
}

LBSInfos& LBSInfos::operator =(const LBSInfos& lbs_basic_info){
	if(lbs_basic_info.data_!=NULL){
		lbs_basic_info.data_->AddRef();
	}

	if(data_!=NULL){
		data_->Release();
	}
	data_ = lbs_basic_info.data_;
	return (*this);
}


}
