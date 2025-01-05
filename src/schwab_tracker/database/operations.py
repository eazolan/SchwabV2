from contextlib import contextmanager
import sqlite3
from typing import List, Dict, Any, Iterator
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

@contextmanager
def database_connection(db_path: Path) -> Iterator[sqlite3.Connection]:
    """Context manager for database connections."""
    conn = None
    try:
        # Ensure the directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)
        
        conn = sqlite3.connect(db_path)
        yield conn
        conn.commit()  # Add commit here before the connection closes
    except sqlite3.Error as e:
        if conn:
            conn.rollback()  # Explicitly rollback on error
        logger.error(f"Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()

class DatabaseManager:
    def __init__(self, config: dict):
        project_root = get_project_root()
        base_dir = project_root / config['database']['base_dir']
        self.stock_db_path = base_dir / config['database']['stock_db_name']
        self.active_stocks_db_path = base_dir / config['database']['active_stocks_db_name']
        self.temp_table = config['database']['temp_table_name']

    def ensure_temp_table_exists(self) -> None:
        """Ensure temporary options table exists and is populated."""
        with database_connection(self.stock_db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='option_chains'
            """)
            
            if not cursor.fetchone():
                raise ValueError("options_chains table does not exist. Please run data collection first.")

            # Create temp table
            cursor.execute(f'DROP TABLE IF EXISTS {self.temp_table}')
            
            # Create and populate temp table with next Friday's options
            cursor.execute(f"""
                CREATE TABLE {self.temp_table} AS 
                WITH next_friday AS (
                    SELECT CASE 
                        -- Get days until next Friday (Friday is 5)
                        WHEN strftime('%w', 'now') <= '5' THEN
                            -- If today is before or on Friday, get this week's Friday
                            date('now', '+' || (5 - CAST(strftime('%w', 'now') AS INTEGER)) || ' days')
                        ELSE
                            -- If today is after Friday, get next week's Friday
                            date('now', '+' || (5 - CAST(strftime('%w', 'now') AS INTEGER) + 7) || ' days')
                        END as date
                )
                SELECT *
                FROM option_chains
                WHERE date(expirationDate) = (SELECT date FROM next_friday)
                AND bid > 0
                AND underlyingPrice > 5
            """)
            
            # Log the date we're using and record count
            cursor.execute(f"SELECT DISTINCT date(expirationDate) FROM {self.temp_table} LIMIT 1")
            next_friday = cursor.fetchone()
            if next_friday:
                logger.info(f"Using options expiring on: {next_friday[0]}")
            else:
                logger.warning("No options found for next Friday")
            
            cursor.execute(f"SELECT COUNT(*) FROM {self.temp_table}")
            row_count = cursor.fetchone()[0]
            logger.info(f"Created temporary options table with {row_count} rows")

            conn.commit()

    def execute_query(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Execute a query and return results as a list of dictionaries."""
        try:
            self.ensure_temp_table_exists()
            
            with database_connection(self.stock_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            raise

    def get_otm_options(self) -> List[Dict[str, Any]]:
        """Get out-of-the-money options from the temporary table."""
        query = f"""
        SELECT symbol, expirationDate, strikePrice, bid, putCall, underlyingPrice
        FROM {self.temp_table}
        WHERE bid > 0
        AND (
            (putCall = 'CALL' AND strikePrice > underlyingPrice)
            OR (putCall = 'PUT' AND strikePrice < underlyingPrice)
        )
        ORDER BY symbol, expirationDate
        """
        return self.execute_query(query)

def get_project_root() -> Path:
    """Get the project root directory."""
    return Path(__file__).resolve().parent.parent.parent.parent