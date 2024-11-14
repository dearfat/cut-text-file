use anyhow::{Context, Ok, Result};
use redis::{cmd, AsyncCommands, Commands};
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use tokio;
use walkdir::WalkDir;
use log::{debug,error,info,warn};
use std::io::{self,BufRead , BufReader, Read};
use crate::config::RedisClient;
use crate::model::book::{PREFIX_QUEUE_BOOK_CDN, BookRedisClient,CHANNEL_PSB_BOOK_TASK};

use std::sync::Arc;

#[allow(dead_code)]
#[derive(Debug)]
pub struct FileProcessor {
	pub input_dir: PathBuf,
	pub output_dir: PathBuf,
	pub redis_client: Arc<RedisClient>,
}

#[allow(dead_code)]
impl FileProcessor {
	pub fn new(input_dir: &str, output_dir: &str, redis_client:Arc<RedisClient>) -> Result<Self> {
			fs::create_dir_all(input_dir)?;
			let mut fp = FileProcessor {
				input_dir: PathBuf::from(input_dir),
				output_dir: PathBuf::from(output_dir),
				redis_client: redis_client,
			}	;
			Ok(fp)
	}
	// ? means return error if any error occurs
	// or return the value ; unpack the value of Result
	// store file name to redis
	#[warn(dependency_on_unit_never_type_fallback)]
	pub async fn store_filenames_to_redis(&self) -> Result<()> {
			let mut conn = self.redis_client.get_connection().await?;
			info!("redis conn2");
			for entry in WalkDir::new(&self.input_dir) {
					let entry = entry?;
					info!("Entry: {:?}", entry.path());
					info!("Entry: {:?}", entry.file_type().is_file());
					if entry.file_type().is_file() {
							let path = entry.path();
							info!("Path: {:?}", path);
							if let Some(filename) = path.file_name() {
								info!("Filename: {:?}", filename);
									if let Some(filename_str) = filename.to_str() {
										info!("Filename: {:?}", filename_str);
											// conn.sadd("files_to_process", filename_str)?;
											conn.sadd(PREFIX_QUEUE_BOOK_CDN, filename_str).await?;
									}
							}
					}
			}
			info!("store filename to redis end");
			Ok(())
	}

	// handle single file
	pub fn process_file(&self, file_path: &Path, name:&str ,stop: i32 ) -> Result<()> {
			if !file_path.exists() {
				// return Err(io::Error::new(io::ErrorKind::NotFound, "File not found").into());
				error!("File not found : {:?}", &file_path);
				return Result::Ok(()); 
			}
			let source_file = if file_path.is_file() {
				file_path.to_path_buf()
			} else {
				file_path.join(name)
			};
			
			let file = File::open(&source_file).context(format!("{}\n{}","open file not exit",&source_file.display()))?;
			let mut reader = BufReader::new(file);
			const DELIMITER:&str = "###";
			let mut idx :i32 = 0;
			let part:Vec<&str> = name.split(".").collect();
			info!("single 1 part name: {:?} \n ",part);
			let canon_path= self.output_dir.join(part[0]);
			if !canon_path.exists(){
				fs::create_dir_all(&canon_path).context("create output directory  err")?;
			}
			let out_path = canon_path.canonicalize().context("ouput dir not eixt")?.to_string_lossy().to_string();
			info!("single 2 part out_path: {:?} \n ",out_path);
			let mut content = String::new();
			let mut line = String::new();
			while reader.read_line(&mut line)? > 0 {
					if idx > stop{
						break;
					}
					if line.starts_with(DELIMITER){
						if content.len() > 0{
							let full_path = Path::new(&out_path).join(format!("{}{}.{}",part[0],idx,part[1]));
							if !full_path.exists(){ // if file not exist , create it or do nothing
								fs::write(full_path, &content)?;
							}
							
							idx += 1;
							content.clear();
							
						}
						line.clear();
					}else{
						content.push_str(&line);
						line.clear();
					}
			}
			Ok(())
	}

	// handle all files
	pub async fn process_all_files(&self)->Result<()>{
			let mut conn = self.redis_client.get_connection().await?;
			let mut bclient = BookRedisClient::new(self.redis_client.clone()).await?;
			let files:Vec<String> = conn.smembers(PREFIX_QUEUE_BOOK_CDN).await?;
			info!("Processing all files {:?}", files);
			for _file in files{
					let  _file: &str = &_file;
					let file = match !_file.contains(".txt") {
						true => format!("{}.txt",_file),
						false => _file.to_string(),
					};
					let input_path = &self.input_dir;
					let fpath = input_path.join(&file);
					info!("Processing all files  file: {:?}", fpath);
					if fpath.exists(){
						info!("exist");
						if let  Some(book) = bclient.get_book_by_source(file.to_string()).await?{
								match self.process_file(&input_path,&file,book.start_count.unwrap_or(0)){
										Result::Ok(())=>{
											//  TODO add map to store the file name
												// conn.srem("files_to_process", &file)?;
												cmd("SRM").arg(PREFIX_QUEUE_BOOK_CDN).arg(&file).query_async(&mut conn).await?;
										
										}
										Err(e)=>{
												error!("Error processing file {:?}: {:?}", input_path, e);
								}
							}
						}
					}
			}
			Ok(())
	}

	pub async fn process_with_retry(&self,input_path: &Path, stop:i32 , max_retries:u32)->Result<()>{
			let mut retries = 0;
			loop {
					let name:&str = input_path.file_name().unwrap().to_str().unwrap_or("");
					match self.process_file(input_path,name,stop){
							Result::Ok(_) =>{
									self.redis_client.get_connection().await?.srem("files_to_process", input_path.file_name().unwrap().to_str().unwrap()).await?;   
									break;
							}
							Err(e)=>{
									retries += 1;
									if retries > max_retries{
											return Err(e);
									}
									tokio::time::sleep(std::time::Duration::from_secs(1)).await;
							}
					}
			}
			Ok(())
	}
}
