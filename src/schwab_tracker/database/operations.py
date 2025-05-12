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
        self.temp_puts_table = "temp_puts_table"
        self.temp_calls_table = "temp_calls_table"

    def ensure_puts_table_exists(self, custom_date=None) -> None:
        """Ensure temporary puts table exists and is populated with options.
        
        Args:
            custom_date (str, optional): A specific date in YYYY-MM-DD format.
                If provided, uses this date instead of calculating next Friday.
        """
        with database_connection(self.stock_db_path) as conn:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='option_chains'
            """)

            if not cursor.fetchone():
                raise ValueError("options_chains table does not exist. Please run data collection first.")

            # Create temp table
            cursor.execute(f'DROP TABLE IF EXISTS {self.temp_puts_table}')

            # Create and populate temp table with either custom date or next Friday's PUT options
            if custom_date:
                # Use the custom date provided by the user
                date_condition = f"date(expirationDate) = '{custom_date}'"
                logger.info(f"Using custom expiration date: {custom_date}")
            else:
                # Calculate next Friday as before
                date_condition = """
                    date(expirationDate) = (
                        SELECT CASE 
                            WHEN strftime('%w', 'now') <= '5' THEN
                                date('now', '+' || (5 - CAST(strftime('%w', 'now') AS INTEGER)) || ' days')
                            ELSE
                                date('now', '+' || (5 - CAST(strftime('%w', 'now') AS INTEGER) + 7) || ' days')
                            END
                    )
                """
                
            # Create the table with the appropriate date condition
            cursor.execute(f"""
                CREATE TABLE {self.temp_puts_table} AS 
                SELECT 
                    symbol,
                    expirationDate,
                    strikePrice,
                    bid,
                    putCall,
                    underlyingPrice,
                    option_symbol,
                    intrinsicValue,
                    daysToExpiration,
                    openInterest,
                    totalVolume,
                    delta,
                    theta,
                    mark
                FROM option_chains
                WHERE {date_condition}
                AND putCall = 'PUT'
                AND bid > 0
                AND underlyingPrice > 5
            """)

            # Log the date we're using and record count
            cursor.execute(f"SELECT DISTINCT date(expirationDate) FROM {self.temp_puts_table} LIMIT 1")
            exp_date = cursor.fetchone()
            if exp_date:
                logger.info(f"Using PUT options expiring on: {exp_date[0]}")
            else:
                logger.warning("No PUT options found for the specified date")

            cursor.execute(f"SELECT COUNT(*) FROM {self.temp_puts_table}")
            row_count = cursor.fetchone()[0]
            logger.info(f"Created temporary PUTS table with {row_count} rows")

            conn.commit()

    def ensure_calls_table_exists(self):
        """Ensure temporary calls table exists and is populated with options."""
        with database_connection(self.stock_db_path) as conn:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='option_chains'
            """)

            if not cursor.fetchone():
                raise ValueError("options_chains table does not exist. Please run data collection first.")

            # Create temp table
            cursor.execute(f'DROP TABLE IF EXISTS {self.temp_calls_table}')

            # Create the table for CALL options - include ALL columns from option_chains
            cursor.execute(f"""
                CREATE TABLE {self.temp_calls_table} AS 
                SELECT 
                    symbol,
                    timestamp,
                    putCall,
                    option_symbol,
                    description,
                    bid,
                    ask,
                    last,
                    mark,
                    bidSize,
                    askSize,
                    totalVolume,
                    openInterest,
                    volatility,
                    delta,
                    gamma,
                    theta,
                    vega,
                    rho,
                    strikePrice,
                    expirationDate,
                    daysToExpiration,
                    inTheMoney,
                    theoreticalOptionValue,
                    timeValue,
                    intrinsicValue,
                    multiplier,
                    underlyingPrice
                FROM option_chains
                WHERE putCall = 'CALL'
                AND bid > 0
                AND underlyingPrice > 5
            """)

            cursor.execute(f"SELECT COUNT(*) FROM {self.temp_calls_table}")
            row_count = cursor.fetchone()[0]
            logger.info(f"Created temporary CALLS table with {row_count} rows")

            conn.commit()


    def execute_query_puts(self, query: str, params: tuple = (), custom_date=None) -> List[Dict[str, Any]]:
        """Execute a query against the puts table and return results as a list of dictionaries."""
        try:
            self.ensure_puts_table_exists(custom_date)

            with database_connection(self.stock_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            raise

    def execute_query_calls(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Execute a query against the calls table and return results as a list of dictionaries."""
        try:
            self.ensure_calls_table_exists()

            with database_connection(self.stock_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            raise

    def get_otm_options(self, custom_date=None) -> List[Dict[str, Any]]:
        """Get out-of-the-money PUT options from the temporary table."""
        query = f"""
        SELECT 
            symbol, 
            expirationDate, 
            strikePrice, 
            bid, 
            putCall, 
            underlyingPrice,
            option_symbol,
            intrinsicValue
        FROM {self.temp_puts_table}
        WHERE bid > 0
        AND underlyingPrice > 5
        AND strikePrice < underlyingPrice
        AND (underlyingPrice - strikePrice) * 100 > bid * 100  -- Ensure it's not too close to being ITM
        ORDER BY symbol, expirationDate
        """
        return self.execute_query_puts(query, custom_date=custom_date)

def get_project_root() -> Path:
    """Get the project root directory."""
    return Path(__file__).resolve().parent.parent.parent.parent