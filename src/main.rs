use tokio;

use molly_db::wrappers;
use molly_db::database_service;

#[tokio::main]
async fn main() {
    // connections
    let client = database_service::connection().await.unwrap();

    // //GET OHLCV data
    // let eod_client = wrappers::create_reqwest_client().await;
    // let ohlcv = wrappers::get_ohlc(&eod_client, "AAPL", "US", "2023-10-01", "2023-10-05").await.unwrap();
    // println!("{:#?}", &ohlcv);

    // // INSERT_MANY records into the database
    // database_service::create_or_insert_many(&client, ohlcv, "equity", "spot", "1d").await;

    // // READ_MANY records from the database
    // let records = database_service::read_many(&client, "2023-10-04", "2023-10-05","equity", "spot", "1d").await;
    // println!("{:#?}", records)
}

// go through file and do error handling
// go thorugh files and add loging info
// create if collection doesnt exist
// update read many filter conditions

