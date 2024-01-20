use crate::database::database_service::MongoDbClient;
use anyhow::Result;
use polars::prelude::*;
use tokio;

fn get_data() -> Result<Vec<(String, DataFrame)>>{
    #[tokio::main]
    async fn get_data() -> Result<Vec<(String, DataFrame)>> {
        let mongo_client = MongoDbClient::new().await;
        let dfs = mongo_client
            .run(vec![
                (
                    "AAPL",
                    "US",
                    "equity_spot_1d",
                    "eod",
                    "1970-01-01",
                    "2024-01-01",
                ),
                (
                    "MSFT",
                    "US",
                    "equity_spot_1d",
                    "eod",
                    "1970-01-01",
                    "2024-01-01",
                ),
                (
                    "AMZN",
                    "US",
                    "equity_spot_1d",
                    "eod",
                    "1970-01-01",
                    "2024-01-01",
                ),
                (
                    "GOOGL",
                    "US",
                    "equity_spot_1d",
                    "eod",
                    "1970-01-01",
                    "2024-01-01",
                ),
            ])
            .await
            .unwrap();
        Ok(dfs)
    }
    get_data()
}

pub fn test() {
    env_logger::init();
    let dfs = get_data().unwrap();
    println!("{:#?}", dfs)
}
