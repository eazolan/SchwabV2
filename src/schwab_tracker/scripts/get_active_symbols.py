import requests
import csv
import logging
import yaml
from io import StringIO
from pathlib import Path
from typing import Dict

from schwab_tracker.utils.logging_config import setup_logging
from schwab_tracker.database.operations import database_connection, get_project_root

logger = logging.getLogger(__name__)

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
        
        # Get database path
        db_path = (get_project_root() / 
                  config['database']['base_dir'] / 
                  config['database']['active_stocks_db_name'])

        # Get and process data
        logger.info("Fetching active stocks from AlphaVantage...")
        csv_data = client.get_active_stocks()
        create_database(csv_data, db_path)
        
        logger.info("Successfully updated active stocks database")

    except Exception as e:
        logger.error(f"Error in get-symbols: {e}")
        raise

if __name__ == "__main__":
    main()