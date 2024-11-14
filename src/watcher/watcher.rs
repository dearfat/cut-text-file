use anyhow::{Ok,Result };
use log::info;
use log::warn;
use redis::Commands;
use std::path::{Path,PathBuf };
use notify::{Watcher, RecursiveMode, Event,EventKind,Result as NotifyResult};
use std::sync::mpsc;
use std::sync::Arc;
use crate::model::book;
use crate::processor::FileProcessor;
use crate::config::RedisClient;
use crate::model::book::{Book,BookRedisClient,PREFIX_QUEUE_BOOK_CDN,CHANNEL_PSB_BOOK_TASK};
use std::collections::HashSet;
use tokio::time::{sleep, Duration};
use futures::stream::StreamExt;
use std::result::Result::{Ok as ResultOk};

#[allow(dead_code)]
#[derive(Debug)]
pub struct FileWatcher{
	pub processor: FileProcessor,
	pub watcher: notify::RecommendedWatcher,
	watcher_rx: mpsc::Receiver<NotifyResult<Event>>,
	known_file: HashSet<PathBuf>,
	check_interval:Duration,
 }
 
 impl FileWatcher{
		pub fn new(input_dir:&str , output_dir:&str , redis_client:Arc<RedisClient>)->Result<Self>{
				 let processor = FileProcessor::new(input_dir, output_dir, redis_client)?;
				 // create channel receive file-system-event
				 let (tx,rx) = mpsc::channel();
				 // create file watcher
				 let mut watcher = notify::recommended_watcher(move|res|{
						 if let Err(e) = tx.send(res){
							warn!("Failed to send sevent:{}",e);
						 }
				 })?;
				 
				 watcher.watch(Path::new(input_dir), RecursiveMode::Recursive)?;
				 Ok(Self{
						 processor,
						 watcher,
						 watcher_rx:rx,
						 known_file: HashSet::new(),
						 check_interval: Duration::new(120, 0), // 120 second 0 nano
				 })
		 }
 
		pub async  fn start_watching(&mut self)->Result<()>{
			let conn = self.processor.redis_client.get_connection().await?;
			let mut brclient = BookRedisClient::new(self.processor.redis_client.clone()).await?;

			let mut pubsub = conn.into_pubsub();
			pubsub.subscribe(CHANNEL_PSB_BOOK_TASK).await?;
			let mut  stream = pubsub.on_message();
			while let Some(msg) = stream.next().await {
				let payload:String = msg.get_payload()?;
				if payload == "exit"{
					break;
				}
				let book_id = payload.parse::<i32>().unwrap();
				info!("Received message: {:?} , id :{:?}",payload,book_id);
				let book = match brclient.get_book_by_id(&book_id).await? {
					Some(book) => book,
					None => {
						warn!("Book not found: {:?}",&book_id);
						continue;
					}
				};
				let file = match book.source_url{
					Some(source) => source.split("/").collect::<Vec<&str>>().last().unwrap_or(&"").to_string(),
					None => {
						warn!("Book source not found: {:?}",&book_id);
						continue;
					}
				};
				let abpath = std::env::current_dir()?.join(&self.processor.input_dir).join(&file);
				info!("abpath in watch start");
				if abpath.exists(){ // source file exists
							self.handle_new_file(&abpath,&file,book.start_count).await?;
				}			
			}

			Ok(())
		 }


		pub async  fn handle_new_file(&self, path: &Path,name:&str,stop:Option<i32>)->Result<()>{
					info!("handleNewFile:{:?}",path);
					if let Some(step) = stop {
						// self.processor.process_with_retry(path).await?;
						self.processor.process_file(path,name,step)?;
					}
				 println!("Processed file: {:?}", name);
				 Ok(())
		 }
 }
 
