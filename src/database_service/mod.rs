// This is the database service layers. The code here
// directly interacts with the database to perform 
// CRUD operations.

use crate::wrappers::{self, Ohlcv};
use bson::{Document, DateTime};
use mongodb::Client;
use dotenv::dotenv;
use polars::prelude::DataFrame;
use std::{env, collections::HashMap};
use std::error::Error;
use serde::{Serialize, Deserialize};
use mongodb::bson::{ oid::ObjectId, doc, Bson };

#[derive(Serialize, Deserialize, Debug)]
pub struct InsertOhlcv {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    id: Option<ObjectId>,
    ticker: String,
    ohlcv: Vec<wrappers::Ohlcv>
}

// pub struct MetaDataStruct {
//     isin: String,
//     ticker: String,
//     source: String,
//     exchange: String
// }

// pub struct InsertTimeseriesStruct {
//     datetime: DateTime,
//     open: f32,
//     high: f32,
//     low: f32,
//     close: f32,
//     volume: i64,
//     metadata: HashMap<String, String>
// }

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

pub async fn create_data(
    ohlcv: Vec<wrappers::Ohlcv>, client: &Client
) -> Result<(), Box<dyn Error>> {
    
    // get collection
    let my_collection: mongodb::Collection<Document> = client.database("testing").collection("mycollection");
    let ohlcv_current = InsertOhlcv {
        id: None,
        ticker: "AAPL".to_string(),
        ohlcv: ohlcv
    };

    // convert to Bson instance
    let ohlcv_serialised = bson::to_bson(&ohlcv_current).unwrap();
    let ohlcv_document = ohlcv_serialised.as_document().unwrap();

    // insert into the collection and extract the inserted id value
    let insert_ohlcv = my_collection.insert_one(ohlcv_document.to_owned(), None).await.unwrap();
    let ohlcv_id = insert_ohlcv
        .inserted_id
        .as_object_id()
        .expect("Retrieved _id should have been of type ObjectId");
    println!("Document ID: {:?}", ohlcv_id);
    Ok(())
}

pub async fn insert_many(records: Vec<Ohlcv>) {
    let client = connection().await;
    let documents: Vec<Document> = records
        .iter()
        .map(|record| {bson::to_document(record).expect("Faied to convert to Bson")})
        .collect();
    let my_collection: mongodb::Collection<Document> = client.database("testing").collection("mycollection");
    let inserted_docs = my_collection.insert_many(documents, None).await;
    println!("{:#?}", inserted_docs.unwrap())
}

// pub async fn create_or_insert_timeseries(
//     dtype: &str, dformat: &str, dfreq: &str, series_metadata: Vec<String>, client: &Client, mut df: DataFrame
// ) -> Result<(), Box<dyn Error>> {
//     // converting df to the desire document form
//     df.as_single_chunk();
//     let mut iters = df.columns(["datetime", "open", "high", "low", "close", "volume"])?
//         .iter().map(|s| s.iter()).collect::<Vec<_>>();



//     // if collection exits, insert

//     // if collection doesn't exist, create one

//     Ok(())
// }


// pub async fn read_data(ticker: String, client: &Client) -> Result<(), Box<dyn Error>> {

//     // get collection
//     let my_collection: mongodb::Collection<Document> = client.database("testing").collection("mycollection");

//     // get document from database
//     let timeseries_data = my_collection
//         .find(Some(doc! { "ticker":  ticker }), None)
//         .await?;
    
//     // Deserialize the document into a Movie instance
//     let retrieved_data: InsertOhlcv = bson::from_bson(Bson::Document(timeseries_data))?;
//     println!("Movie loaded from collection: {:?}", retrieved_data);
//     Ok(())
// }