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

    def ensure_puts_table_exists(self) -> None:
        """Ensure temporary puts table exists and is populated with next Friday's options."""
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

            # Create and populate temp table with next Friday's PUT options
            cursor.execute(f"""
                CREATE TABLE {self.temp_puts_table} AS 
                WITH next_friday AS (
                    SELECT CASE 
                        WHEN strftime('%w', 'now') <= '5' THEN
                            date('now', '+' || (5 - CAST(strftime('%w', 'now') AS INTEGER)) || ' days')
                        ELSE
                            date('now', '+' || (5 - CAST(strftime('%w', 'now') AS INTEGER) + 7) || ' days')
                        END as date
                )
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
                WHERE date(expirationDate) = (SELECT date FROM next_friday)
                AND putCall = 'PUT'
                AND bid > 0
                AND underlyingPrice > 5
            """)

            # Log the date we're using and record count
            cursor.execute(f"SELECT DISTINCT date(expirationDate) FROM {self.temp_puts_table} LIMIT 1")
            next_friday = cursor.fetchone()
            if next_friday:
                logger.info(f"Using PUT options expiring on: {next_friday[0]}")
            else:
                logger.warning("No PUT options found for next Friday")

            cursor.execute(f"SELECT COUNT(*) FROM {self.temp_puts_table}")
            row_count = cursor.fetchone()[0]
            logger.info(f"Created temporary PUTS table with {row_count} rows")

            conn.commit()

    def ensure_calls_table_exists(self) -> None:
        """Ensure temporary calls table exists and is populated with 90 days of options."""
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

            # Create and populate temp table with 90 days of CALL options
            cursor.execute(f"""
                CREATE TABLE {self.temp_calls_table} AS 
                SELECT *
                FROM option_chains
                WHERE date(expirationDate) >= date('now')
                AND date(expirationDate) <= date('now', '+90 days')
                AND putCall = 'CALL'
                AND bid > 0
                AND underlyingPrice > 5
            """)

            # Log the date range and record count
            cursor.execute(f"""
                SELECT 
                    MIN(date(expirationDate)) as min_date,
                    MAX(date(expirationDate)) as max_date,
                    COUNT(DISTINCT date(expirationDate)) as date_count
                FROM {self.temp_calls_table}
            """)
            date_info = cursor.fetchone()
            if date_info:
                logger.info(
                    f"Using CALL options from {date_info[0]} to {date_info[1]} ({date_info[2]} expiration dates)")
            else:
                logger.warning("No CALL options found in the next 90 days")

            cursor.execute(f"SELECT COUNT(*) FROM {self.temp_calls_table}")
            row_count = cursor.fetchone()[0]
            logger.info(f"Created temporary CALLS table with {row_count} rows")

            conn.commit()

    def execute_query_puts(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Execute a query against the puts table and return results as a list of dictionaries."""
        try:
            self.ensure_puts_table_exists()

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

    def get_otm_options(self) -> List[Dict[str, Any]]:
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
        return self.execute_query_puts(query)



def get_project_root() -> Path:
    """Get the project root directory."""
    return Path(__file__).resolve().parent.parent.parent.parent