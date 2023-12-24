// use molly_db::database_service;
use molly_db::securities::Equities;
// use molly_db::wrappers::WrapperFunctions;

#[tokio::main]
async fn main() {
    let equities_client = Equities::new().await;
    let equity_generals = equities_client
        .batch_get_ticker_generals(
            vec![("AAPL", "US"), ("AAPL", "US"), ("AAPL", "US")]
        );
    println!("{:#?}", equity_generals.await.unwrap());
    // let equities_ohlcv = equities_client.batch_get_intraday_data_equities(
    //     vec![
    //         ("AAPL", "US"),
    //         ("AAPL", "US"),
    //         ("AAPL", "US"),
    //         ("AAPL", "US"),
    //         ("AAPL", "US"),
    //         ("AAPL", "US"),
    //         ("AAPL", "US"),
    //         ("AAPL", "US"),
    //         ("AAPL", "US"),
    //         ("AAPL", "US"),
    //         ("AAPL", "US"),
    //     ],
    //     "2023-10-30",
    //     "2023-11-01",
    //     "5m",
    // );
    // println!("EQITIES {:?}", equities_ohlcv.await.unwrap());

    // let equities_live = equities_client
    //     .batch_get_live_lagged_data_equity(vec![
    //         ("AAPL", "US"),
    //         ("AAPL", "US"),
    //         ("AAPL", "US"),
    //         ("AAPL", "US"),
    //         ("AAPL", "US"),
    //         ("AAPL", "US"),
    //         ("AAPL", "US"),
    //         ("AAPL", "US"),
    //         ("AAPL", "US"),
    //         ("AAPL", "US"),
    //         ("AAPL", "US"),
    //     ])
    //     .await;
    // println!("EQUITIES LIVE {:?}", &equities_live.unwrap());

    // let wrapper_client = WrapperFunctions::new().await;
    // let wrapper_intra = wrapper_client
    //     .batch_get_intraday_data(
    //         vec![
    //             ("AAPL", "US"),
    //             ("AAPL", "US"),
    //             ("AAPL", "US"),
    //             ("AAPL", "US"),
    //             ("AAPL", "US"),
    //             ("AAPL", "US"),
    //             ("AAPL", "US"),
    //             ("AAPL", "US"),
    //             ("AAPL", "US"),
    //             ("AAPL", "US"),
    //             ("AAPL", "US"),
    //         ],
    //         "2023-10-30",
    //         "2023-10-31",
    //         "5m",
    //     )
    //     .await;
    // println!("WRAPPER INTRA {:?}", wrapper_intra.unwrap());

    // let wrapper_fundamental = wrapper_client
    //     .batch_get_fundamental_data(
    //         vec![("AAPL", "US"), ("AAPL", "US"), ("AAPL", "US")],
    //     );
    // println!("{:#?}", wrapper_fundamental.await.unwrap());
}

// FINISH BATCH GET METADATA INFO
// THEN INTEGRATE THIS INTO THE OTHER FUNCTION CALLS




// use molly_db::database_service;
// use molly_db::wrappers;

// use env_logger::Env;
// use molly_db::wrappers::OhlcvMetadata;
// use tokio;

// #[tokio::main]
// async fn main() {
//     // configure env_logger
//     env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
//     // let request_client = wrappers::create_reqwest_client().await;

//     // connections
//     let client = database_service::connection()
//         .await
//         .expect("Failed to establish connection to DB");

//     //GET OHLCV data
//     let eod_client = wrappers::create_reqwest_client().await;
//     let ohlcv = wrappers::get_ohlc(&eod_client, "AAPL", "US", "2023-10-01", "2023-11-20")
//         .await
//         .unwrap();
//     // println!("{:#?}", &ohlcv);

//     // INSERT_MANY records into the database
//     let _ = database_service::insert_timeseries(
//         &client,
//         ohlcv,
//         "equity",
//         "spot",
//         "1d",
//         database_service::OhlcGranularity::Hours,
//     )
//     .await;

//     // READ_MANY records from the database
//     let metadata: OhlcvMetadata = wrappers::metadata_info().await.unwrap();
//     let records = database_service::read_timeseries(
//         &client,
//         "2023-10-04",
//         "2023-11-01",
//         metadata,
//         "equity",
//         "spot",
//         "1d",
//         database_service::OhlcGranularity::Hours,
//     )
//     .await;
//     println!("{:#?}", records.unwrap());

//     // GET TICKER FUNDAMENTALS
//     // let fundmental_doc = wrappers::get_ticker_generals(&request_client, "AAPL", "US").await;
//     // println!("{:#?}", fundmental_doc.unwrap())
// }

// // change api key to be demo
// // make more apis
// // finishh insert_doc() function and test it
// // start read_doc() function
// // change String -> some smart pointer
// // standardise error messages e.g {:#?} or {:?} and format!()
// // standardise function outputs ie Result<()>
