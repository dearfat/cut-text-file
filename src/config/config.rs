use config::{Config,ConfigError,Environment,File};
use serde::Deserialize;
use std::path::{Path,PathBuf};
use log::{debug,error,info,warn};
use anyhow::{Context,Result};
#[allow(dead_code)]
#[derive(Debug,serde::Deserialize)]
pub struct Settings{
    pub file_processing:FileProcessConfig,
    pub redis:RedisConfig,
    pub watcher:WatcherConfig,
}

#[allow(dead_code)]
#[derive(Debug,serde::Deserialize)]
pub struct FileProcessConfig{
    pub input_dir:String,
    pub output_dir:String,
    pub supported_ext:Vec<String>,
}

#[allow(dead_code)]
#[derive(Debug,serde::Deserialize)]
pub struct RedisConfig{
    pub url:String,
    pub pass:Option<String>,
    pub key_prefix:String,
}

#[allow(dead_code)]
#[derive(Debug,serde::Deserialize)]
pub struct WatcherConfig{
    pub recursive:bool,
    pub concurrent_limit:usize,
}

impl Settings{
  pub  fn new() -> Result<Settings,ConfigError>{
        let env = std::env::var("Run_MODE").unwrap_or_else(|_|"dev".to_string());
        let s = Config::builder()
        .add_source(File::with_name("config/default"))
        .add_source(File::with_name(&format!("config/{}",env)).required(false))
        .add_source(Environment::with_prefix("APP"))
        .build()?;
        // info!("Setting: {:?}",s);
        s.try_deserialize()
    }
}

impl FileProcessConfig{
    pub fn input_path(&self)->Result<PathBuf>{
        self.normalize_path(&self.input_dir)
    }

    pub fn output_path(&self)->Result<PathBuf>{
        self.normalize_path(&self.output_dir)
    }

    fn normalize_path(&self,path:&str)->Result<PathBuf>{
        let path = Path::new(path);
        let full_path = std::env::current_dir()?.join(path);
        let absolute_path = if path.is_absolute(){
            path.to_path_buf()
        }else{
            full_path.canonicalize().context("Faild to resolve absolute path")?
        };
        Ok(absolute_path)
    }
}

#[allow(dead_code)]
pub struct RedisClient{
    pub client:redis::Client,
    pub pass:Option<String>,
    pub key_prefix:String,
}

impl RedisClient{
    pub fn new(url:&str , pass: Option<String>,key_prefix:String)->Result<Self>{
        Ok(Self{
         client: redis::Client::open(url)?,
         pass,   
         key_prefix,
        })
    }
    pub async  fn get_connection(&self)->Result<redis::aio::Connection,redis::RedisError>{
        let mut conn = self.client.get_async_connection().await?;
        if let Some(ref pass) = self.pass{ 
            redis::cmd("AUTH").arg(pass).query_async(&mut conn).await?
        }
        Ok(conn)
    }
}

impl std::fmt::Debug for RedisClient{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RedisClient {{ client: {:?}, pass: {:?}, key_prefix: {:?} }}", self.client, self.pass, self.key_prefix)
    }
}
