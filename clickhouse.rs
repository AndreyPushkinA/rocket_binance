use serde::Deserialize;
use reqwest::Error;
use std::time::Duration;
use chrono::NaiveDateTime;
use chrono::Utc;
use std::thread::sleep;

const CLICKHOUSE_ENDPOINT: &str = "http://localhost:8123/";

#[derive(Deserialize)]
struct TickerPrice {
    symbol: String,
    price: String,
}

#[derive(Deserialize)]
struct OrderBook {
    lastUpdateId: u64,
    bids: Vec<[String; 2]>, 
    asks: Vec<[String; 2]>, 
}

#[derive(Deserialize)]
struct RecentTrade {
    id: u64,
    price: String,
    qty: String,
    time: u64,
}

fn main() {
    loop {
        if let Err(err) = fetch_and_print_data() {
            eprintln!("Error: {}", err);
        }

        sleep(Duration::from_millis(500)); // Sleep for 1 second before fetching again
    }
}

fn fetch_and_print_data() -> Result<(), Error> {
    let symbol = "BTCUSDT";
    let ticker_url = format!("https://api.binance.com/api/v3/ticker/price?symbol={}", symbol);
    let order_book_url = format!("https://api.binance.com/api/v3/depth?symbol={}&limit=10", symbol);
    let recent_trades_url = format!("https://api.binance.com/api/v3/trades?symbol={}&limit=20", symbol);

    let ticker_response: TickerPrice = reqwest::blocking::get(&ticker_url)?.json()?;
    let order_book_response: OrderBook = reqwest::blocking::get(&order_book_url)?.json()?;
    let recent_trades_response: Vec<RecentTrade> = reqwest::blocking::get(&recent_trades_url)?.json()?;

    println!("Price of {}: {}", ticker_response.symbol, ticker_response.price);

    let current_time = Utc::now().naive_utc();
    insert_price_into_clickhouse(&current_time, &ticker_response.price);

    println!("Top 10 bids:");
    for bid in &order_book_response.bids {
        println!("Price: {}, Quantity: {}", bid[0], bid[1]);
        insert_bid_into_clickhouse(&current_time, &bid[0], &bid[1]);
    }

    println!("Top 10 asks:");
    for ask in &order_book_response.asks {
        println!("Price: {}, Quantity: {}", ask[0], ask[1]);
        insert_bid_into_clickhouse(&current_time, &ask[0], &ask[1]);
    }

    println!("Last 20 trades:");
    for trade in &recent_trades_response {
        println!("Trade ID: {}, Price: {}, Quantity: {}", trade.id, trade.price, trade.qty);

        let timestamp = NaiveDateTime::from_timestamp(trade.time as i64 / 1000, 0);
        insert_trade_into_clickhouse(&timestamp, &trade.id, &trade.price, &trade.qty);
    }

    Ok(())
}

fn insert_trade_into_clickhouse(timestamp: &NaiveDateTime, id: &u64, price: &str, amount: &str) {
    let client = reqwest::blocking::Client::new();
    let query = format!(
        "INSERT INTO btc_trades (timestamp, id, price, amount) VALUES ('{}', '{}', '{}', '{}')",
        timestamp.format("%Y-%m-%d %H:%M:%S"), 
        id,
        price,
        amount
    );

    let response = client.post(CLICKHOUSE_ENDPOINT)
        .body(query)
        .send()
        .expect("Failed to send request");

    if response.status().is_success() {
        println!("Trade data inserted successfully");
    } else {
        eprintln!("Error inserting trade data: {}", response.status());
    }
}

fn insert_price_into_clickhouse(timestamp: &NaiveDateTime, price: &str) {
    let client = reqwest::blocking::Client::new();
    let query = format!(
        "INSERT INTO btc_price (timestamp, price) VALUES ('{}', '{}')",
        timestamp.format("%Y-%m-%d %H:%M:%S"), 
        price
    );

    let response = client.post(CLICKHOUSE_ENDPOINT)
        .body(query)
        .send()
        .expect("Failed to send request");

    if response.status().is_success() {
        println!("Price data inserted successfully");
    } else {
        eprintln!("Error inserting price data: {}", response.status());
    }
}


fn insert_bid_into_clickhouse(timestamp: &NaiveDateTime, price: &str, quantity: &str) {
    let client = reqwest::blocking::Client::new();
    let query = format!(
        "INSERT INTO btc_bids (timestamp, price, quantity) VALUES ('{}', '{}', '{}')",
        timestamp.format("%Y-%m-%d %H:%M:%S"), 
        price,
        quantity
    );

    let response = client.post(CLICKHOUSE_ENDPOINT)
        .body(query)
        .send()
        .expect("Failed to send request");

    if response.status().is_success() {
        println!("Bid data inserted successfully");
    } else {
        eprintln!("Error inserting bid data: {}", response.status());
    }
}

fn insert_ask_into_clickhouse(timestamp: &NaiveDateTime, price: &str, quantity: &str) {
    let client = reqwest::blocking::Client::new();
    let query = format!(
        "INSERT INTO btc_asks (timestamp, price, quantity) VALUES ('{}', '{}', '{}')",
        timestamp.format("%Y-%m-%d %H:%M:%S"), 
        price,
        quantity
    );

    let response = client.post(CLICKHOUSE_ENDPOINT)
        .body(query)
        .send()
        .expect("Failed to send request");

    if response.status().is_success() {
        println!("Ask data inserted successfully");
    } else {
        eprintln!("Error inserting ask data: {}", response.status());
    }
}
