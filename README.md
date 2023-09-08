# rocket_binance

Rust project to collect price, trades, asks and bids from Binance and save them to Clickhouse. The rast_api.rs creates Rast API using Rocket listening on localhost:8000, routes /btc_price, /btc_asks, /btc_bids and btc_trades
