use anyhow::Result;
use config::Settings;
use config::RedisClient;
use processor::FileProcessor;
use watcher::FileWatcher;
use tokio;
use std::path::{PathBuf,Path};
use std::sync::{Arc};
use std::cell::RefCell;
use log::{debug,error,info,warn};
use redis::{cmd as redisCmd, RedisResult};
mod config;
mod processor;
mod watcher;
mod model;

#[allow(dead_code)]
#[tokio::main]
async fn main() ->Result<()>{
    env_logger::init();
    let settings = Settings::new()?;
    let input_dir = settings.file_processing.input_path()?;
    let output_dir = settings.file_processing.output_path()?;
    info!("Input Dir: {:?}",input_dir);
    info!("Output Dir: {:?}",output_dir);

    let redis_client = Arc::new(RedisClient::new(settings.redis.url.as_str(),settings.redis.pass,settings.redis.key_prefix)?);
    let mut conn = redis_client.get_connection().await?;
    let processor = FileProcessor::new(
        &(input_dir.to_string_lossy().to_string()),
        &(output_dir.to_string_lossy().to_string()), 
        redis_client.clone()
    )?;    
    let  rst:Option<Vec<String>> = redisCmd("keys").arg("book:*").query_async(&mut conn).await?;
    info!("Keys: {:?}",rst);
    handle_init_book_files(rst,redis_client.clone(),&processor).await?;
    // info!("Redis Config: {:?}",redis_client);
    
    // processor.store_filenames_to_redis()?;
    // processor.process_all_files().await?;

   // create wahcher
    let mut watcher = FileWatcher::new(
        &(input_dir.to_string_lossy().to_string()),
        &(output_dir.to_string_lossy().to_string()), 
        redis_client.clone(),
    )?;
    // start watching
    watcher.start_watching().await?;
    Ok(())
}

// 在最开始时候，由于没有列表或者我们功能是后置的
// 所以需要把所有的文件名字，依照现有处理逻辑
// 放在redis的一个set(uuid,book_id) , set(source_name,book_id)中，任务名称放在Set队列中
// 设置好队列，然后每隔一段时间，检查一次队列中的文件
async fn  handle_init_book_files(book_names :Option<Vec<String>>,redis_client:Arc<RedisClient>,processor:&FileProcessor) ->Result<()>{
    let  bcliend = RefCell::new(model::book::BookRedisClient::new(redis_client).await?);
    if let Some(items) = book_names {
        let mut iter = items.iter();
        while let Some(item) = iter.next() {
            let full:Vec<&str> = item.split("book:").collect();
            if let Some(id) = full.last() {
                let id = id.parse::<i32>().unwrap_or(-1);
                if id < 0 {
                    continue;
                }
                let mut bc = bcliend.borrow_mut();
               if let Some(book) =  bc.get_book_by_id(&id).await?{
                    match book.source_url{ 
                        Some(source)=>{
                            let  j = source.split("/").collect::<Vec<&str>>().last().unwrap_or(&"").to_string();
                            info!("Get Name From Source: {:?}",&source);
                            // let mut bc: std::cell::RefMut<'_, model::BookRedisClient> = bcliend.borrow_mut();
                            info!("Book name: {:?}",j);
                            // source -file absolute path
                            let _j = std::env::current_dir()?.join(&processor.input_dir).join(&j);
                            info!("Book source absolute file path: {:?}",&_j);
                            let file_path = Path::new(&_j);
                            processor.process_file(file_path, &j, book.start_count.unwrap_or(0))?;
                        },
                        None=>{
                            info!("Book not found by id: {:?}",id);
                        }
                          
                     } 
               }else{
                     error!("Book not found by id: {:?}",id);
               }
              
            }

        }
    }
    Ok(())
}
