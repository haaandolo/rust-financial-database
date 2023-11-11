use crate::utility_functions::string_to_datetime;
use crate::wrappers::Ohlcv;

use mongodb::{ Client, Collection, bson, options::{CreateCollectionOptions, TimeseriesGranularity, TimeseriesOptions}};
use dotenv::dotenv;
use std::env;
use bson::doc;
use futures::StreamExt;
use anyhow::Result;

const DATABASE_NAME: &str = "molly_db";

pub enum OhlcGranularity {
    Hours,
    Minutes,
    Seconds
}

pub async fn connection() -> Result<Client>  {
    dotenv().ok();
    let client_url = env::var("MONGODB_URI").expect("Could not parse MongoDB URI from .env");
    let client = Client::with_uri_str(client_url).await.expect("Could not create MongoDB Client from DB URI");
    log::info!("Established Client for MongoDB!");
    Ok(client)
}

pub async fn create_or_insert_many(client: &Client, records: Vec<Ohlcv>, dtype: &str, dformat: &str, dfreq: &str, granularity: OhlcGranularity) -> Result<()> {
    let db = client.database(DATABASE_NAME);

    // create collection if it doest exist in DB
    let collection_name = format!("{dtype}_{dformat}_{dfreq}");
    let collections = db.list_collection_names(None).await.expect("Failed to list collection");

    match collections.contains(&collection_name) {
        true => (),
        false => {
            log::error!("{}", format!("Collection {collection_name} does not exist"));
            let timeseries_options = TimeseriesOptions::builder()
                .time_field("datetime".to_string())
                .meta_field(Some("metadata".to_string()))
                .granularity(
                    match granularity {
                        OhlcGranularity::Hours => Some(TimeseriesGranularity::Hours),
                        OhlcGranularity::Minutes => Some(TimeseriesGranularity::Minutes),
                        OhlcGranularity::Seconds => Some(TimeseriesGranularity::Seconds),
                    }
                )
                .build();
            let options = CreateCollectionOptions::builder()
                .timeseries(timeseries_options)
                .build();
            db.create_collection(&collection_name, Some(options)).await
                .expect("Failed to create timeseries collection");
            log::info!("{}", format!("Sucessfully create timeseries collection: {}", &collection_name))
        }
    }
    
    // insert if collection exists
    let my_collection: Collection<Ohlcv> = db.collection(&collection_name);
    my_collection.insert_many(records, None)
        .await
        .expect(format!("Failed to insert OHLCV to {} collection", &collection_name).as_str());
    log::info!("Successfully inserted collection");
    Ok(())
}

pub async fn read_many(client: &Client, start_date: &str, end_date: &str, dtype: &str, dformat: &str, dfreq: &str) -> Result<Vec<Ohlcv>> {
    let collection_name = format!("{dtype}_{dformat}_{dfreq}");
    let my_collection: Collection<Ohlcv> = client.database(DATABASE_NAME).collection(&collection_name);
    let start_date = string_to_datetime(start_date).await;
    let end_date = string_to_datetime(end_date).await;
    let filter = doc! { "metadata.data_type": "stock",
        "datetime": { 
            "$gte": start_date,
            "$lte": end_date
        }
    };
    let mut cursor = my_collection.find(filter, None).await?;
    let mut results: Vec<Ohlcv> = Vec::new();
    while let Some(my_collection) = cursor.next().await {
        results.push(my_collection?)
    }
    log::info!("Sucessfully read documennts from DB");
    Ok(results)
}
