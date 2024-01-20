# Quant Database

### High Level Overview
This is my implementation of a quant database. How it works is that the user will input the tickers 
with the correct params (see the System Standards section below). Behind the scene the system will
determine if the series exists in the current database. If it does not, it will fire a request to 
the specified data source api to get the data from 1970 to the current date. We use 1970 as a cut off
date as mongo db cannot store timeseries data that go back further than this. The system will then
insert the data to the desire collection then read the data from that collection only retrieving 
data from the "from" and "to" params specified. The system will then put the read data into a 
polars dataframe for further data manipulation.

### System Standards
User will input data they want in this standdard (ticker, exchange, collection, source, from, to).
An example of this can be found below:

```plaintext
(
    "AAPL",
    "US",
    "equity_spot_1d",
    "eod",
    "2023-10-10",
    "2024-01-01",
),
(
    "BTC-USD",
    "CC",
    "crypto_spot_1d",
    "eod",
    "2023-10-10",
    "2024-01-01",
),
```

This then gets serialised into the MongoTickerParams struct. This struct is passed around the system 
to do everything from get data from api sources, check collection exists, insert and read data.

Another major standard the system uses is the OhlcvMetaData stuct. The TimeseriesMetaDataStruct has
a lot of overlapping fields to the OhlcvMetaData. For example, both structs have fields such as
"collection_name", "ticker", "source" and "exchange", hence these fields can be used to filter the
relevant rows in the collection. For example, we can look at the TimeseriesMetaDataStruct for 1 day
spot Apple and use "APPL", "exchange", "eod" and "equity_spot_1d" to filter the all the documents 
belonging to Apple from the equity_spot_1d collection. You can essentially think of the the overlapping
fields within the TimeseriesMetaDataStruct and OhlcvMetaData stuct as the keys that you join on in 
a regular SQL database.

### How to Add New Datasource to the System
1. Make new file in the data_apis folder i.e., binance.rs
2. Within that file make functions to get the relevant data from that source
3. Make sure if it its timeseries data it obeys the standard outlined above i.e., each series rows
has a OhlcvMetaData and and TimeseriesMetaDataStruct associated with it.
4. Add the source name to the get_data_from_apis() function in the mongodb.rs file so it can sort 
the urls by datasource.

### Note on testing

### Note on Dates for Series (VERIFY ALL THIS IS CORRECT)
- Data comes in as string object from datasource api in either date or datetime. If it is a date the 
system converts it to a datetime for consistency.
- When inserting data the system converts sting date/datetime to UTC datetime then converts to bson datetime
- When reading, it reads in bson datetime, converts to utc datetime then converts to a nativedatetime.
