// This is the database service layers. The code here
// directly interacts with the database to perform 
// CRUD operations.

use crate::wrappers::Ohlcv;
use mongodb::{ Client, Collection };
use dotenv::dotenv;
use polars::prelude::DataFrame;
use std::env;
use bson::doc;
use futures::StreamExt;
use polars::prelude::*;


pub async fn connection() -> Client {
    dotenv().ok();
    let client_url = env::var("MONGODB_URL")
        .expect("You must set the MONGODB_URL environment var!");
    let client = Client::with_uri_str(client_url)
        .await
        .unwrap_or_else(|_| panic!("Error establishing MongoDB Client"));
    println!("Established Client for MongoDB!");
    return client
}

pub async fn insert_many(records: Vec<Ohlcv>) {
    let client = connection().await;
    let my_collection: Collection<Ohlcv> = client.database("testing").collection("mycollection");
    let inserted_docs = my_collection.insert_many(records, None).await;
    println!("{:#?}", inserted_docs.unwrap())
}

pub async fn read_many(client: &Client) -> Vec<Ohlcv> {
    let my_collection: Collection<Ohlcv> = client.database("testing").collection("mycollection");
    let filter = doc! { "volume": doc!{"$gt": 10}};
    let mut cursor = my_collection.find(filter, None).await.unwrap();
    let mut results: Vec<Ohlcv> = Vec::new();
    while let Some(my_collection) = cursor.next().await {
        results.push(my_collection.unwrap())
    }
    return results
}

pub async fn insert_many2(df: DataFrame) {
    let client = connection().await;
    let my_collection: Collection<Ohlcv> = client.database("testing").collection("mycollection");

    

    let inserted_docs = my_collection.insert_many(df, None).await;
    println!("{:#?}", inserted_docs.unwrap())
}