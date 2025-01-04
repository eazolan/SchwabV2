import os
import logging
from pathlib import Path
import yaml
from dotenv import load_dotenv
import time
from datetime import datetime
import sqlite3

from schwab_tracker.api.schwab_client import SchwabClient
from schwab_tracker.database.operations import DatabaseManager, database_connection
from schwab_tracker.utils.logging_config import setup_logging

logger = logging.getLogger(__name__)

def get_quotes(client, db_manager):
    """Get quotes for all active symbols and store in database."""
    try:
        # Connect to ActiveStocks database to get symbols
        with database_connection(db_manager.active_stocks_db_path) as conn_source:
            cursor_source = conn_source.cursor()
            cursor_source.execute("SELECT symbol FROM all_active_stocks")
            all_symbols = [row[0] for row in cursor_source.fetchall()]

        print(f"Found {len(all_symbols)} symbols in {db_manager.active_stocks_db_path}")

        # Create/Connect to BasicStockData database
        with database_connection(db_manager.stock_db_path) as conn_dest:
            cursor_dest = conn_dest.cursor()

            # Drop the table if it exists
            cursor_dest.execute('DROP TABLE IF EXISTS stock_data')
            print("Dropped existing stock_data table")

            # Create new table
            cursor_dest.execute('''
                CREATE TABLE stock_data (
                    symbol TEXT,
                    timestamp DATETIME,
                    asset_main_type TEXT,
                    asset_sub_type TEXT,
                    quote_type TEXT,
                    
                    fund_avg_10day_volume REAL,
                    fund_avg_1year_volume REAL,
                    quote_bid_price REAL,
                    quote_total_volume INTEGER,
                    
                    PRIMARY KEY (symbol, timestamp)
                )
            ''')

            # Process symbols in batches
            batch_size = 300
            total_processed = 0

            for i in range(0, len(all_symbols), batch_size):
                batch = all_symbols[i:i + batch_size]
                print(f"\nProcessing batch {(i//batch_size)+1} of {(len(all_symbols)-1)//batch_size + 1}")

                try:
                    response = client.get_quotes(batch)
                    quotes_data = response.json()

                    # Handle invalid symbols from the errors object
                    if 'errors' in quotes_data:
                        error_info = quotes_data.pop('errors')
                        if 'invalidSymbols' in error_info:
                            invalid_symbols = error_info['invalidSymbols']
                            print(f"Invalid symbols: {', '.join(invalid_symbols)}")

                    timestamp = datetime.now().isoformat()
                    
                    # Process each quote
                    for symbol, data in quotes_data.items():
                        try:
                            fundamental = data.get('fundamental', {})
                            quote = data.get('quote', {})

                            cursor_dest.execute('''
                                INSERT INTO stock_data (
                                    symbol, timestamp, asset_main_type, asset_sub_type,
                                    quote_type, fund_avg_10day_volume, fund_avg_1year_volume,
                                    quote_bid_price, quote_total_volume
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                symbol,
                                timestamp,
                                data.get('assetMainType'),
                                data.get('assetSubType'),
                                data.get('quoteType'),
                                fundamental.get('avg10DaysVolume'),
                                fundamental.get('avg1YearVolume'),
                                quote.get('bidPrice'),
                                quote.get('totalVolume')
                            ))

                            total_processed += 1

                        except sqlite3.Error as e:
                            print(f"Error inserting data for {symbol}: {e}")

                    conn_dest.commit()
                    print(f"Processed {total_processed} symbols so far...")

                    # Add a small delay to avoid hitting rate limits
                    time.sleep(0.5)

                except Exception as e:
                    print(f"Error getting quotes for batch: {e}")
                    continue

            print(f"\nCompleted processing {total_processed} symbols")

    except Exception as e:
        print(f"Error in get_quotes: {e}")
        raise
        
        
def populate_options_table(client, db_manager):
    """Populate options data table with chains for high-volume stocks."""
    start_time = time.time()

    print("\nPopulating options table...")

    try:
        # Changed db_path to stock_db_path to match DatabaseManager class
        with database_connection(db_manager.stock_db_path) as conn:
            cursor = conn.cursor()

            # Get high volume stocks (> 1M volume)
            cursor.execute("""
            SELECT DISTINCT symbol 
            FROM stock_data 
            WHERE fund_avg_10day_volume > 1000000 AND quote_bid_price > 5
            ORDER BY fund_avg_10day_volume DESC
            """)

            high_volume_symbols = [row[0] for row in cursor.fetchall()]
            print(f"Found {len(high_volume_symbols)} stocks with volume > 1M and > 5$")

            # Create new options table
            cursor.execute('DROP TABLE IF EXISTS option_chains')
            cursor.execute('''
                CREATE TABLE option_chains (
                    symbol TEXT,
                    timestamp DATETIME,
                    putCall TEXT,
                    option_symbol TEXT,
                    description TEXT,
                    bid REAL,
                    ask REAL,
                    last REAL,
                    mark REAL,
                    bidSize INTEGER,
                    askSize INTEGER,
                    totalVolume INTEGER,
                    openInterest INTEGER,
                    volatility REAL,
                    delta REAL,
                    gamma REAL,
                    theta REAL,
                    vega REAL,
                    rho REAL,
                    strikePrice REAL,
                    expirationDate TEXT,
                    daysToExpiration INTEGER,
                    inTheMoney BOOLEAN,
                    theoreticalOptionValue REAL,
                    timeValue REAL,
                    intrinsicValue REAL,
                    multiplier REAL,
                    underlyingPrice REAL,
                    PRIMARY KEY (option_symbol, timestamp)
                )
            ''')

            total_processed = 0
            for symbol in high_volume_symbols:
                try:
                    print(f"\nProcessing {symbol}...")

                    response = client.get_option_chains(
                        symbol,
                        contractType="ALL",
                        strikeCount=10,
                        strategy="SINGLE",
                        includeUnderlyingQuote=True
                    )
                    
                    chain_data = response.json()
                    timestamp = datetime.now().isoformat()

                    # Process call options
                    call_map = chain_data.get('callExpDateMap', {})
                    for date_key, strikes in call_map.items():
                        for strike_price, options in strikes.items():
                            for option in options:
                                cursor.execute('''
                                    INSERT INTO option_chains (
                                        symbol, timestamp, putCall, option_symbol, description,
                                        bid, ask, last, mark, bidSize, askSize, totalVolume,
                                        openInterest, volatility, delta, gamma, theta, vega,
                                        rho, strikePrice, expirationDate, daysToExpiration,
                                        inTheMoney, theoreticalOptionValue, timeValue,
                                        intrinsicValue, multiplier, underlyingPrice
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                ''', (
                                    symbol, timestamp, option.get('putCall'),
                                    option.get('symbol'), option.get('description'),
                                    option.get('bid'), option.get('ask'),
                                    option.get('last'), option.get('mark'),
                                    option.get('bidSize'), option.get('askSize'),
                                    option.get('totalVolume'), option.get('openInterest'),
                                    option.get('volatility'), option.get('delta'),
                                    option.get('gamma'), option.get('theta'),
                                    option.get('vega'), option.get('rho'),
                                    float(strike_price), option.get('expirationDate'),
                                    option.get('daysToExpiration'), option.get('inTheMoney'),
                                    option.get('theoreticalOptionValue'),
                                    option.get('timeValue'), option.get('intrinsicValue'),
                                    option.get('multiplier'), chain_data.get('underlyingPrice')
                                ))

                    # Process put options
                    put_map = chain_data.get('putExpDateMap', {})
                    for date_key, strikes in put_map.items():
                        for strike_price, options in strikes.items():
                            for option in options:
                                cursor.execute('''
                                    INSERT INTO option_chains (
                                        symbol, timestamp, putCall, option_symbol, description,
                                        bid, ask, last, mark, bidSize, askSize, totalVolume,
                                        openInterest, volatility, delta, gamma, theta, vega,
                                        rho, strikePrice, expirationDate, daysToExpiration,
                                        inTheMoney, theoreticalOptionValue, timeValue,
                                        intrinsicValue, multiplier, underlyingPrice
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                ''', (
                                    symbol, timestamp, option.get('putCall'),
                                    option.get('symbol'), option.get('description'),
                                    option.get('bid'), option.get('ask'),
                                    option.get('last'), option.get('mark'),
                                    option.get('bidSize'), option.get('askSize'),
                                    option.get('totalVolume'), option.get('openInterest'),
                                    option.get('volatility'), option.get('delta'),
                                    option.get('gamma'), option.get('theta'),
                                    option.get('vega'), option.get('rho'),
                                    float(strike_price), option.get('expirationDate'),
                                    option.get('daysToExpiration'), option.get('inTheMoney'),
                                    option.get('theoreticalOptionValue'),
                                    option.get('timeValue'), option.get('intrinsicValue'),
                                    option.get('multiplier'), chain_data.get('underlyingPrice')
                                ))

                    conn.commit()
                    total_processed += 1
                    print(f"Processed options for {symbol}")

                    # Add a small delay to avoid hitting rate limits
                    time.sleep(0.5)

                except Exception as e:
                    print(f"Error processing options for {symbol}: {e}")
                    continue

            print(f"\nCompleted processing options for {total_processed} symbols")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        elapsed_time = time.time() - start_time
        print(f"\nTotal execution time: {elapsed_time:.2f} seconds")

def main():
    """Main entry point for data collection."""
    try:
        # Load config (remove load_dotenv call)
        config = load_config()
        setup_logging(config)

        # Initialize components with config values
        client = SchwabClient(
            config,
            config['api']['schwab']['app_key'],
            config['api']['schwab']['app_secret'],
            config['api']['schwab']['callback_url']
        )
        db_manager = DatabaseManager(config)

        # Run data collection
        print("\nCollecting stock quotes...")
        get_quotes(client, db_manager)

        # Then get options data
        print("\nCollecting options data...")
        populate_options_table(client, db_manager)

    except Exception as e:
        logger.error(f"Error in data collection: {e}")
        raise

def load_config():
    """Load configuration from YAML file."""
    config_path = Path(__file__).resolve().parent.parent.parent.parent / 'config' / 'config.yml'
    with open(config_path) as f:
        return yaml.safe_load(f)

if __name__ == "__main__":
    main()