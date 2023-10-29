use tokio;

use molly_db::wrappers;
// use molly_db::database_service;

#[tokio::main]
async fn main() {
    //GET OHLCV data
    let eod_client = wrappers::create_reqwest_client().await;
    let ohlcv = wrappers::get_ohlc(&eod_client, "AAPL", "US", "2023-10-01", "2023-10-15").await;
    println!("{:#?}", ohlcv.unwrap())

    // INSERT_MANY records into the database
}