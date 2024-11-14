use std::collections::HashMap;
use log::info;
use serde::{de::Error, Deserialize, Serialize};
use serde_json::error;
use uuid::Uuid;
use anyhow::{Context, Ok, Result};
use redis::{cmd, Commands};
use crate::config::RedisClient;
use std::sync::{Arc};

#[allow(dead_code)]
#[derive(Debug , Serialize , Deserialize)]
pub struct Book {
	#[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i32>,
    pub uuid: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avatar: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keywords: Option<Vec<String>>,
    #[serde(default)]
    pub scores: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chapter_count: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default)]
    pub online: bool,
    #[serde(default)]
    pub is_complete: bool,
    #[serde(default = "default_true")]
    pub is_new: bool,
    #[serde(default)]
    pub is_recommend: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub words_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chapter_price: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_book_name: Option<String>,
    #[serde(default)]
    pub categories: Option<Vec<u32>>,
    #[serde(default)]
    pub pop_count: i32,
    pub created_at: i64,
    pub updated_at: i64,
    pub deleted_at: i64,
    pub category_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_count: Option<i32>,
    #[serde(default)]
    pub is_fav: bool,
		pub chapter:Option<Chapter>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Chapter{
	id:String,
	book_id:i32,
	book_name:Option<String>,
	chapter_list:Vec<ChapterItem>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChapterItem{
	id:String,
	chapter_id:i32,
	chapter_name:String,
	content:Option<String>,
	require_vip:bool,
	price:i32,
}


fn default_true()->bool{
	true
}

pub const PREFIX_BOOK:&str = "book:";
pub const PREFIX_BOOK_UUID:&str ="book:uuid:";
pub const PREFIX_BOOK_SOURCE:&str   = "book:source:";
pub const PREFIX_QUEUE_BOOK_CDN:&str = "queue:book:cdn";
// pub const PREFIX_QUEUE_BOOK_STATE:&str= "queue:book:map";
pub const CHANNEL_PSB_BOOK_TASK:&str = "channel:psb:book:task";
impl Book{

	pub fn to_redis_json(&self)->Result<String,anyhow::Error>{
		serde_json::to_string(&self).context("Failed to serialize book")
	}
	
	pub fn from_redis_json(json:&str)->Result<Self,anyhow::Error>{
		serde_json::from_str(json).context("Failed to deserialize book")
	}
}

pub struct BookRedisClient{
	conn: redis::aio::Connection,
}

impl BookRedisClient{

	pub async fn new(redis_client: Arc<RedisClient>) -> Result<Self, anyhow::Error> {
		let conn = redis_client.get_connection().await?;
		Ok(Self { conn })
	}

	pub async fn set_book(&mut self, book:&Book)->Result<(),anyhow::Error>{
		let json = book.to_redis_json()?;
		let key = format!("{}{}",&PREFIX_BOOK , book.id.unwrap_or(0));
		redis::cmd("JSON.SET")
			.arg(&key)
			.arg("$")
			.arg(json)
			.query_async(&mut self.conn)
			.await?;

		Ok(())
	}

	pub async fn get_book_by_id(&mut self, id:&i32)->Result<Option<Book>,anyhow::Error>{
		let key = format!("{}{}",&PREFIX_BOOK,id);
		let json:Option<String> = cmd("JSON.GET")
			.arg(&key)
			.arg("$")
			.query_async(&mut self.conn).await?;
		info!("Get Book By Id: {:?}",json);
		match json{
			Some(j)=> {
				let v:serde_json::Value = serde_json::from_str(&j)?;
				if let Some(ele) = v.get(0){
					let j = ele.to_string();
					Ok(Some(Book::from_redis_json(&j)?))
				}else{
					Ok(None)
				}
			},
			None =>Ok(None)
		}
	}
	pub async fn update_book_field<T:Serialize>(
		&mut self,
		id:&i32,
		field:&str,
		value:T,
	)->Result<(),anyhow::Error>{
		let key = format!("{}{}",&PREFIX_BOOK,id);
		let json = serde_json::to_string(&value)?;
		redis::cmd("JSON.SET")
			.arg(&key)
			.arg(field)
			.arg(json)
			.query_async(&mut self.conn).await?;
		Ok(())
	}

	pub async fn get_book_id_by_uuid(&mut self , uuid:&str)->Result<Option<i32>,anyhow::Error>{
		let key = format!("{}{}",&PREFIX_BOOK_UUID,uuid);
		let json:Option<i32> = redis::cmd("GET").arg(key).query_async(&mut self.conn).await?;
			Ok(json)
	}

	pub async fn get_book_by_source(&mut self , nname:String)->Result<Option<Book>,anyhow::Error>{

		let mut name = nname;
		if !name.contains(".txt") {
			name = format!("{}.txt",name).to_string();
		}
		let key = format!("{}{}",&PREFIX_BOOK_SOURCE,name);
		let json:Option<i32> = redis::cmd("GET").arg(key).query_async(&mut self.conn).await?;
		if let Some(j) = json{
			 let key1 = format!("{}{}",&PREFIX_BOOK,j);
			 let json1:Option<String> =  cmd("JSON.GET")
					.arg(&key1)
					.arg("$")
					.query_async(&mut self.conn).await?;
			match json1{
					Some(jk) => {
						let v:serde_json::Value = serde_json::from_str(&jk)?;
						if let Some(ele) = v.get(0){
							let j = ele.to_string();
							Ok(Some(Book::from_redis_json(&j)?))
						}else{
							Ok(None)
						}
					},
					None => Ok(None),
			 }
		}else{
			Ok(None)
		}
	}

	pub async fn set_book_source(&mut self, name:&str,id:&i32)->Result<(),anyhow::Error>{
		let key = format!("{}{}",&PREFIX_BOOK_SOURCE,name);
		redis::cmd("SET")
			.arg(&key)
			.arg(id)
			.query_async(&mut self.conn).await?;
		Ok(())
	}

	pub async fn set_book_uuid(&mut self, uuid:&str,id:&i32)->Result<(),anyhow::Error>{
		let key = format!("{}{}",&PREFIX_BOOK_UUID,uuid);
		redis::cmd("SET")
			.arg(&key)
			.arg(id)
			.query_async(&mut self.conn).await?;
		Ok(())
	}

	pub async fn get_book_uuid(&mut self, uuid:&str)->Result<Option<i32>,anyhow::Error>{
		let key = format!("{}{}",&PREFIX_BOOK_UUID,uuid);
		let json:Option<i32> = redis::cmd("GET").arg(key).query_async(&mut self.conn).await?;
		Ok(json)
	}

	pub async fn get_book_by_uuid(&mut self, uuid:&str)->Result<Option<Book>,anyhow::Error>{
		let key = format!("{}{}",&PREFIX_BOOK_UUID,uuid);
		let json:Option<i32> = redis::cmd("GET").arg(key).query_async(&mut self.conn).await?;
		if let Some(j) = json{
			let key1 = format!("{}{}",&PREFIX_BOOK,j);
			let json1:Option<String> = cmd("JSON.GET")
			.arg(&key1)
			.arg("$")
			.query_async(&mut self.conn).await?;
			let _ = match json1 {
				Some(jk) => {
					let v:serde_json::Value = serde_json::from_str(&jk)?;
					if let Some(ele) = v.get(0){
						let j = ele.to_string();
						Ok(Some(Book::from_redis_json(&j)?))
					}else{
						Ok(None)
					}
				} ,
				None => Ok(None),
			};
		}
		Ok(None)
	}

	pub async fn push_to_queue(&mut self, name:&str)->Result<(),anyhow::Error>{
		let name = name.split('/').collect::<Vec<&str>>().last().unwrap_or(&"").to_string();
		redis::cmd("SADD")
			.arg(PREFIX_QUEUE_BOOK_CDN)
			.arg(name)
			.query_async(&mut self.conn).await?;
		Ok(())
	}
}

