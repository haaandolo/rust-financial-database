use crate::utility_functions::string_to_datetime;

use crate::wrappers::Ohlcv;
use mongodb::{ Client, Collection, bson };
use dotenv::dotenv;
use std::env;
use bson::doc;
use futures::StreamExt;
use anyhow::Result;

const DATABASE_NAME: &str = "molly_db";

pub async fn connection() -> Result<Client>  {
    dotenv().ok();
    let client_url = env::var("MONGODB_URI").expect("Could not parse MongoDB URI");
    let client = Client::with_uri_str(client_url).await.expect("Could not create MongoDB Client");
    println!("Established Client for MongoDB!"); // change this to log? 
    Ok(client)
}

pub async fn create_or_insert_many(client: &Client, records: Vec<Ohlcv>, dtype: &str, dformat: &str, dfreq: &str) {
    // insert functionailty to create collection if it doest exist or insert if it does
    // >>>>>>>HERE<<<<<<<<<<<

    let collection_name = format!("{}_{}_{}", dtype, dformat, dfreq);
    let my_collection: Collection<Ohlcv> = client.database(DATABASE_NAME).collection(&collection_name);
    let inserted_docs = my_collection.insert_many(records, None).await;
    println!("{:#?}", inserted_docs.unwrap())
}

pub async fn read_many(client: &Client, start_date: &str, end_date: &str, dtype: &str, dformat: &str, dfreq: &str) -> Vec<Ohlcv> {
    let collection_name = format!("{}_{}_{}", dtype, dformat, dfreq);
    let my_collection: Collection<Ohlcv> = client.database(DATABASE_NAME).collection(&collection_name);
    let start_date = string_to_datetime(start_date).await;
    let end_date = string_to_datetime(end_date).await;
    let filter = doc! { "metadata.data_type": "stock",
        "datetime": { 
            "$gte": start_date,
            "$lte": end_date
        }
    };
    let mut cursor = my_collection.find(filter, None).await.unwrap();
    let mut results: Vec<Ohlcv> = Vec::new();
    while let Some(my_collection) = cursor.next().await {
        results.push(my_collection.unwrap())
    }
    return results
}
