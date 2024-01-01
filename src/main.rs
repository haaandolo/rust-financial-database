// use molly_db::database_service;
// use molly_db::securities::{Equities, Crypto};
// use molly_db::wrappers::WrapperFunctions;
use molly_db::database_service::mongodb::MongoDbClient;

#[tokio::main]
async fn main() {
    let mongo_client = MongoDbClient::new().await;
    let create_collections = mongo_client
        .run(vec![
            // ("AAPL", "US", "1970-01-01", "equity_spot_1d", "eod"),
            // ("AAPL", "US", "2023-10-01", "equity_spot_1d", "eod"),
            // ("AAPL", "US", "2023-10-01", "equity_spot_1d", "binance"),
            // ("BTC-USD", "CC", "2023-10-01", "crypto_spot_1d", "fb"),
            // ("BTC-USD", "CC", "2023-10-01", "crypto_spot_1d", "eod"),
            // ("BTC-USD", "CC", "2023-10-01", "crypto_spot_1d", "binance"),
            ("AAPL", "US", "2022-10-01", "equity_spot_1m", "eod"),
            // ("AAPL", "US", "2000-10-01", "equity_spot_1h", "eod"),
            // ("AAPL", "US", "2023-10-01", "equity_spot_live", "eod"),
            // ("BTC-USD", "CC", "2023-10-01", "crypto_spot_live", "eod"),
        ])
        .await;
    println!("{:#?}", create_collections);
}

// DEFINE FLOW OF OPERTIONS FOR MAIN FUNCTION FOR DB
// INSERT DATAFRAME BUT FILTER FOR ONLY REQUIRED ROWS
// MAKE SYSTEM BE DEPENDENT ON METADATA COLLECTION
// RESEARCH FROM AND TO IN LIVE DATA API
