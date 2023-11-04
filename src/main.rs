use tokio;

use molly_db::wrappers;
use molly_db::database_service;

#[tokio::main]
async fn main() {
    //GET OHLCV data
    let eod_client = wrappers::create_reqwest_client().await;
    let ohlcv = wrappers::get_ohlc(&eod_client, "AAPL", "US", "2023-10-01", "2023-10-05").await.unwrap();
    println!("{:#?}", &ohlcv);

    // INSERT_MANY records into the database
    database_service::insert_many(ohlcv).await;

    // READ_MANY records from the database
    let client = database_service::connection().await;
    let records = database_service::read_many(&client).await;
    println!("{:#?}", records)
}

// figure out how to insert many rows from polars df into mongo db