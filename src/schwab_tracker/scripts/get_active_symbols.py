import requests
import csv
import logging
import yaml
from io import StringIO
from pathlib import Path
from typing import Dict
from datetime import datetime

from schwab_tracker.utils.logging_config import setup_logging
from schwab_tracker.database.operations import database_connection, get_project_root

logger = logging.getLogger(__name__)

SP500_CSV_URL = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"

class AlphaVantageClient:
    """Client for interacting with AlphaVantage API."""
    
    def __init__(self, config: Dict):
        self.api_key = config['api']['alphavantage']['key']
        self.base_url = config['api']['alphavantage']['base_url']

    def get_active_stocks(self) -> str:
        """Get list of active stocks from AlphaVantage."""
        params = {
            'function': 'listing_status',
            'state': 'active',
            'apikey': self.api_key
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            return response.content.decode('utf-8')
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching stock data: {e}")
            raise

def create_database(csv_data: str, db_path: Path) -> None:
    """Create and populate the active stocks database."""
    try:
        # Ensure the directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)

        with database_connection(db_path) as conn:
            cursor = conn.cursor()

            # Drop the table if it exists and create new one
            cursor.execute('DROP TABLE IF EXISTS all_active_stocks')
            logger.info("Dropped existing active_stocks table")

            logger.info("Creating new active_stocks table")
            cursor.execute('''
            CREATE TABLE all_active_stocks (
                symbol TEXT PRIMARY KEY,
                assetType TEXT 
            )
            ''')

            # Read and insert the CSV data
            csv_reader = csv.DictReader(StringIO(csv_data))
            for row in csv_reader:
                cursor.execute('''
                INSERT INTO all_active_stocks (symbol, assetType)
                VALUES (?, ?)
                ''', (
                    row['symbol'],
                    row['assetType']
                ))

            # Print summary
            cursor.execute('SELECT COUNT(*) FROM all_active_stocks')
            total_stocks = cursor.fetchone()[0]
            logger.info(f"Database populated with {total_stocks} active stocks")

    except Exception as e:
        logger.error(f"Error creating database: {e}")
        raise

def fetch_sp500_constituents() -> str:
    """Fetch S&P 500 constituents from GitHub."""
    try:
        response = requests.get(SP500_CSV_URL)
        response.raise_for_status()
        return response.content.decode('utf-8')
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching S&P 500 data: {e}")
        raise

def create_index_memberships_table(db_path: Path) -> None:
    """Create the index_memberships table if it doesn't exist."""
    with database_connection(db_path) as conn:
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='index_memberships'
        """)
        
        if not cursor.fetchone():
            logger.info("Creating index_memberships table")
            cursor.execute('''
                CREATE TABLE index_memberships (
                    symbol TEXT NOT NULL,
                    index_name TEXT NOT NULL,
                    added_date DATE,
                    removed_date DATE,
                    PRIMARY KEY (symbol, index_name, added_date)
                )
            ''')
            conn.commit()
            logger.info("index_memberships table created")
        else:
            logger.info("index_memberships table already exists")

def update_sp500_memberships(csv_data: str, db_path: Path) -> None:
    """Update S&P 500 memberships in the database."""
    try:
        with database_connection(db_path) as conn:
            cursor = conn.cursor()
            
            # Get current S&P 500 members from database
            cursor.execute("""
                SELECT symbol FROM index_memberships 
                WHERE index_name = 'SP500' 
                AND removed_date IS NULL
            """)
            existing_members = {row[0] for row in cursor.fetchall()}
            
            # Parse CSV and get new members
            csv_reader = csv.DictReader(StringIO(csv_data))
            current_members = set()
            new_additions = 0
            
            current_date = datetime.now().date().isoformat()
            
            for row in csv_reader:
                symbol = row['Symbol']
                current_members.add(symbol)
                
                # Add new member if not already in database
                if symbol not in existing_members:
                    cursor.execute('''
                        INSERT INTO index_memberships (symbol, index_name, added_date, removed_date)
                        VALUES (?, ?, ?, NULL)
                    ''', (symbol, 'SP500', current_date))
                    new_additions += 1
                    logger.info(f"Added {symbol} to SP500")
            
            # Mark removed members
            removed_members = existing_members - current_members
            removals = 0
            
            for symbol in removed_members:
                cursor.execute("""
                    UPDATE index_memberships 
                    SET removed_date = ? 
                    WHERE symbol = ? 
                    AND index_name = 'SP500' 
                    AND removed_date IS NULL
                """, (current_date, symbol))
                removals += 1
                logger.info(f"Marked {symbol} as removed from SP500")
            
            conn.commit()
            
            # Print summary
            cursor.execute("""
                SELECT COUNT(*) FROM index_memberships 
                WHERE index_name = 'SP500' 
                AND removed_date IS NULL
            """)
            total_members = cursor.fetchone()[0]
            
            logger.info(f"S&P 500 update complete:")
            logger.info(f"  Total current members: {total_members}")
            logger.info(f"  New additions: {new_additions}")
            logger.info(f"  Removals: {removals}")
            
    except Exception as e:
        logger.error(f"Error updating S&P 500 memberships: {e}")
        raise

def load_config() -> Dict:
    """Load configuration from YAML file."""
    config_path = get_project_root() / 'config' / 'config.yml'
    with open(config_path) as f:
        return yaml.safe_load(f)

def main():
    """Main entry point for getting active symbols."""
    try:
        # Setup
        config = load_config()
        setup_logging(config)

        # Initialize client
        client = AlphaVantageClient(config)
        
        # Get database paths
        active_stocks_db_path = (get_project_root() / 
                                config['database']['base_dir'] / 
                                config['database']['active_stocks_db_name'])
        stock_db_path = (get_project_root() / 
                        config['database']['base_dir'] / 
                        config['database']['stock_db_name'])

        # Get and process active stocks data
        logger.info("Fetching active stocks from AlphaVantage...")
        csv_data = client.get_active_stocks()
        create_database(csv_data, active_stocks_db_path)
        
        logger.info("Successfully updated active stocks database")
        
        # Update index memberships
        logger.info("\nUpdating index memberships...")
        create_index_memberships_table(stock_db_path)
        
        logger.info("Fetching S&P 500 constituents from GitHub...")
        sp500_csv_data = fetch_sp500_constituents()
        
        logger.info("Updating S&P 500 memberships...")
        update_sp500_memberships(sp500_csv_data, stock_db_path)
        
        logger.info("\nAll updates completed successfully")

    except Exception as e:
        logger.error(f"Error in get-symbols: {e}")
        raise

if __name__ == "__main__":
    main()