// use molly_db::database_service;
// use molly_db::securities::{Equities, Crypto};
// use molly_db::wrappers::WrapperFunctions;
use molly_db::data_apis::EodApi;
use molly_db::database_service::mongodb::MongoDbClient;

#[tokio::main]
async fn main() {
    let mongo_client = MongoDbClient::new().await;
    let create_collections = mongo_client.read_series(vec![
        ("AAPL", "US", "2023-10-01", "equity_spot_1d", "eod"),
        ("AAPL", "US", "2023-10-01", "equity_spot_1d", "eod"),
        ("AAPL", "US", "2023-10-01", "equity_spot_1d", "binance"),
        ("BTC-USD", "CC", "2023-10-01", "crypto_spot_1d", "fb"),
        ("BTC-USD", "CC", "2023-10-01", "crypto_spot_1d", "eod"),
        ("BTC-USD", "CC", "2023-10-01", "crypto_spot_1d", "binance"),
        ("AAPL", "US", "2023-10-01", "equity_spot_5m", "eod"),
        ("AAPL", "US", "2023-10-01", "equity_spot_1h", "eod"),
        ("AAPL", "US", "nodate", "equity_spot_live", "eod"),
        ("BTC-USD", "CC", "nodate", "crypto_spot_live", "eod"),
    ]).await;
    println!("{:#?}", create_collections);

}

// INSERT DATAFRAME BUT FILTER FOR ONLY REQUIRED ROWS