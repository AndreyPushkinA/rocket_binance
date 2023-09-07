extern crate chrono;
use chrono::NaiveDateTime;
use rocket::serde::json::Json;
use rocket::http::Status;
use reqwest::Error;
use chrono::Utc;
use serde::{Serialize, Deserialize};
use rocket::get;
use rocket::routes;

#[derive(Deserialize, Debug)]
struct TickerPrice {
    symbol: String,
    price: String,
}

#[derive(Serialize, Debug)]
struct BtcPrice {
    price: String,
    time: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct RecentTrade {
    id: i64,
    price: String,
    qty: String,
    time: i64,
}

#[derive(Serialize, Debug)]
struct BtcTrades {
    trades: Vec<RecentTrade>,
    time: String,
}

#[derive(Deserialize, Debug)]
struct OrderBook {
    lastUpdateId: i64,
    bids: Vec<Vec<String>>,
    asks: Vec<Vec<String>>,
}

#[derive(Serialize, Debug)]
struct Bids {
    bids: Vec<Vec<String>>
}

#[derive(Serialize, Debug)]
struct Asks {
    asks: Vec<Vec<String>>
}

#[get("/btc_price")]
async fn get_btc_price() -> Result<Json<BtcPrice>, Status> {
    match fetch_btc_price().await {
        Ok(price) => Ok(Json(price)),
        Err(_) => Err(Status::InternalServerError),
    }
}

#[get("/btc_trades")]
async fn get_btc_trades() -> Result<Json<BtcTrades>, Status> {
    match fetch_btc_trades().await {
        Ok(trades) => Ok(Json(trades)),
        Err(_) => Err(Status::InternalServerError),
    }
}

#[get("/btc_bids")]
async fn get_btc_bids() -> Result<Json<Bids>, Status> {
    match fetch_order_book_data().await {
        Ok((bids_data, _)) => Ok(Json(bids_data)),
        Err(_) => Err(Status::InternalServerError),
    }
}

#[get("/btc_asks")]
async fn get_btc_asks() -> Result<Json<Asks>, Status> {
    match fetch_order_book_data().await {
        Ok((_, asks_data)) => Ok(Json(asks_data)),
        Err(_) => Err(Status::InternalServerError),
    }
}

async fn fetch_order_book_data() -> Result<(Bids, Asks), Error> {
    let symbol = "BTCUSDT";

    let order_book_url = format!("https://api.binance.com/api/v3/depth?symbol={}&limit=10", symbol);
    let order_book_response: OrderBook = reqwest::get(&order_book_url).await?.json().await?;
    
    let bids_data = Bids {
        bids: order_book_response.bids,
    };

    let asks_data = Asks {
        asks: order_book_response.asks,
    };
   
    Ok((bids_data, asks_data))
}

async fn fetch_btc_price() -> Result<BtcPrice, Error> {
    let symbol = "BTCUSDT";
    let ticker_url = format!("https://api.binance.com/api/v3/ticker/price?symbol={}", symbol);

    let ticker_response: TickerPrice = reqwest::get(&ticker_url).await?.json().await?;
    let current_time = Utc::now().naive_utc();

    insert_price_into_clickhouse(&current_time, &ticker_response.price);

    Ok(BtcPrice {
        price: ticker_response.price,
        time: current_time.to_string(),
    })
}

async fn fetch_btc_trades() -> Result<BtcTrades, Error> {
    let symbol = "BTCUSDT";
    let recent_trades_url = format!("https://api.binance.com/api/v3/trades?symbol={}&limit=20", symbol);

    let recent_trades_response: Vec<RecentTrade> = reqwest::get(&recent_trades_url).await?.json().await?;
    let current_time = Utc::now().naive_utc().to_string();

    Ok(BtcTrades {
        trades: recent_trades_response,
        time: current_time,
    })
}

#[rocket::main]
async fn main() {
    rocket::build()
        .mount("/", routes![get_btc_price, get_btc_trades, get_btc_asks, get_btc_bids])
        .launch()
        .await;
}
