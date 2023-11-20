use molly_db::database_service;
use molly_db::wrappers;

use molly_db::wrappers::OhlcvMetadata;
use tokio;
use env_logger::Env;

#[tokio::main]
async fn main() {
    // configure env_logger
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    // connections
    let client = database_service::connection().await.expect("Failed to establish connection to DB");

    //GET OHLCV data
    let eod_client = wrappers::create_reqwest_client().await;
    let ohlcv = wrappers::get_ohlc(&eod_client, "AAPL", "US", "2023-07-01", "2023-11-20").await.unwrap();
    // println!("{:#?}", &ohlcv);

    // INSERT_MANY records into the database
    let _ = database_service::insert_timeseries(&client, ohlcv, "equity", "spot", "1d", database_service::OhlcGranularity::Hours).await;

    // READ_MANY records from the database
    let metadata: OhlcvMetadata = wrappers::metadata_info().await;
    let _ = database_service::read_timeseries(&client, "2023-10-04", "2023-11-01", metadata,"equity", "spot", "1d", database_service::OhlcGranularity::Hours).await;
    // println!("{:#?}", records.unwrap())
}

// write funtionaity to insert doc and read doc