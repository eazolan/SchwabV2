from typing import List, Dict, Any, NamedTuple
from decimal import Decimal, ROUND_DOWN
from dataclasses import dataclass
import logging
from collections import OrderedDict

logger = logging.getLogger(__name__)


@dataclass
class OptionMetrics:
    delta: float = None
    theta: float = None
    annual_return: Decimal = None
    days_to_expiry: int = None
    symbol: str = None
    expiration: str = None
    option_type: str = None
    strike: Decimal = None
    contracts: int = None
    premiums: Decimal = None
    exercise: Decimal = None

    @property
    def profit_potential(self) -> Decimal:
        """Calculate potential profit from the option."""
        if self.exercise is None:
            return self.premiums
        return self.premiums + self.exercise


@dataclass
class CoveredCallMetrics(OptionMetrics):
    @property
    def roi_if_called(self) -> Decimal:
        """Calculate ROI if the option is exercised."""
        if self.exercise is None or self.exercise == 0:
            return Decimal('0')
        return (self.premiums + self.exercise) / self.exercise * 100


class OptionsAnalyzer:
    def __init__(self, db_manager, include_nonstandard=False, custom_date=None):
        self.db = db_manager
        self.include_nonstandard = include_nonstandard
        self.custom_date = custom_date
        logger.info(f"OptionsAnalyzer initialized with include_nonstandard={include_nonstandard}, custom_date={custom_date}")

    def get_otm_options(self) -> List[Dict[str, Any]]:
        """Fetch out-of-the-money options from database."""
        # First query to get all options without filtering non-standard ones
        base_query = f"""
            SELECT 
                symbol, 
                expirationDate, 
                strikePrice, 
                bid, 
                putCall, 
                underlyingPrice,
                option_symbol,
                intrinsicValue
            FROM {self.db.temp_puts_table}
            WHERE bid > 0
            AND strikePrice < underlyingPrice
        """

        # Get total count before filtering
        all_options = self.db.execute_query_puts(base_query, custom_date=self.custom_date)
        logger.info(f"Found {len(all_options)} total options before filtering")

        if not self.include_nonstandard:
            # Add filter for non-standard options using GLOB to check for numbers
            standard_query = base_query + """
            AND NOT SUBSTR(option_symbol, 1, 6) GLOB '*[0-9]*'
            """

            # Get filtered results
            results = self.db.execute_query_puts(standard_query, custom_date=self.custom_date)
            filtered_count = len(results)

            # Log the filtering results
            excluded_count = len(all_options) - filtered_count
            if excluded_count > 0:
                logger.info(f"Filtered out {excluded_count} non-standard options")

                # Log examples of what was filtered out
                excluded_query = f"""
                    SELECT DISTINCT symbol, option_symbol, putCall, intrinsicValue
                    FROM {self.db.temp_puts_table}
                    WHERE bid > 0
                    AND SUBSTR(option_symbol, 1, 6) GLOB '*[0-9]*'
                    AND strikePrice < underlyingPrice
                """
                excluded_examples = self.db.execute_query_puts(excluded_query, custom_date=self.custom_date)
                if excluded_examples:
                    logger.info("Examples of excluded non-standard options:")
                    for ex in excluded_examples:
                        logger.info(
                            f"  {ex['symbol']}: {ex['option_symbol']} ({ex['putCall']}) IV:{ex['intrinsicValue']}")

            logger.info(f"Returning {filtered_count} standard options for analysis")
            return results
        else:
            logger.info("Including non-standard options in analysis")
            logger.info(f"Returning all {len(all_options)} options for analysis")
            return all_options

    def get_best_covered_calls(self, symbol: str) -> List[Dict[str, Any]]:
        """
        Analyze covered call options for a specific stock.

        Args:
            symbol: The stock symbol to analyze

        Returns:
            List of call options for the best strike price, showing all expiration dates up to 90 days
        """
        # First, get the current stock price
        query = f"""
            SELECT underlyingPrice
            FROM {self.db.temp_calls_table}
            WHERE symbol = ?
            LIMIT 1
        """
        result = self.db.execute_query_calls(query, (symbol,))
        if not result:
            logger.error(f"No data found for symbol {symbol}")
            return []

        underlying_price = Decimal(str(result[0]['underlyingPrice']))
        logger.info(f"Analyzing covered calls for {symbol} at price ${underlying_price}")

        # Find the closest strike price below current price
        strike_query = f"""
            SELECT DISTINCT strikePrice
            FROM {self.db.temp_calls_table}
            WHERE symbol = ?
            AND putCall = 'CALL'
            AND strikePrice <= ?
            ORDER BY strikePrice DESC
            LIMIT 1
        """
        strike_result = self.db.execute_query_calls(strike_query, (symbol, float(underlying_price)))
        if not strike_result:
            logger.error(f"No valid strike prices found for {symbol}")
            return []

        target_strike = Decimal(str(strike_result[0]['strikePrice']))
        logger.info(f"Selected strike price: ${target_strike}")

        # Get all options at this strike price
        base_query = f"""
            SELECT 
                symbol,
                expirationDate,
                strikePrice,
                bid,
                ask,
                putCall,
                underlyingPrice,
                option_symbol,
                daysToExpiration,
                openInterest,
                totalVolume,
                delta,
                theta
            FROM {self.db.temp_calls_table}
            WHERE symbol = ?
            AND putCall = 'CALL'
            AND strikePrice = ?
            AND daysToExpiration > 0
            AND bid > 0
        """

        if not self.include_nonstandard:
            options_query = base_query + """
            AND NOT SUBSTR(option_symbol, 1, 6) GLOB '*[0-9]*'
            ORDER BY expirationDate ASC
            """
        else:
            options_query = base_query + """
            ORDER BY expirationDate ASC
            """

        results = self.db.execute_query_calls(options_query, (symbol, float(target_strike)))

        if results:
            logger.info(f"Found {len(results)} valid covered call options for analysis")
            for opt in results:
                logger.info(f"Option: {opt['expirationDate']} - ${opt['strikePrice']} "
                            f"(bid: ${opt['bid']}, Delta: {opt['delta']}, OI: {opt['openInterest']})")
        else:
            logger.warning(f"No valid covered call options found for {symbol} "
                           f"at strike ${target_strike}")

        return results

    def calculate_metrics(self, option: Dict[str, Any], available_funds: Decimal) -> OptionMetrics:
        """Calculate relevant metrics for an option contract."""
        symbol = option['symbol']
        expiration = option['expirationDate']
        strike = Decimal(str(option['strikePrice']))
        bid = Decimal(str(option['bid']))
        option_type = option['putCall']
        stock_price = Decimal(str(option['underlyingPrice']))

        # Calculate number of contracts possible with available funds
        if option_type == 'PUT':
            contract_cost = strike * 100
        else:  # CALL
            contract_cost = stock_price * 100

        contracts = int((available_funds // contract_cost))

        # Calculate premiums and potential exercise value
        premiums = bid * 100 * contracts

        if option_type == 'PUT':
            difference = (stock_price - strike) * contracts * 100
        else:  # CALL
            difference = abs(strike - stock_price) * contracts * 100

        return OptionMetrics(
            symbol=symbol,
            expiration=expiration,
            option_type=option_type,
            strike=strike,
            contracts=contracts,
            premiums=premiums,
            exercise=difference
        )


class OptionsScreener:
    def __init__(self, analyzer: OptionsAnalyzer):
        self.analyzer = analyzer
        self.max_results = None  # Will be set from command line argument

    def find_best_options(self, available_funds: Decimal) -> Dict[str, List[OptionMetrics]]:
        """Screen for the best options based on available funds."""
        try:
            options_data = self.analyzer.get_otm_options()

            # Calculate metrics for all options
            all_metrics = [
                self.analyzer.calculate_metrics(option, available_funds)
                for option in options_data
            ]

            # Split and process CALL and PUT options separately
            calls = [m for m in all_metrics if m.option_type == 'CALL']
            puts = [m for m in all_metrics if m.option_type == 'PUT']

            # Get best option per symbol
            best_calls = self._get_best_by_symbol(calls)
            best_puts = self._get_best_by_symbol(puts)

            # Sort by premium potential
            best_calls.sort(key=lambda x: x.premiums, reverse=True)
            best_puts.sort(key=lambda x: x.premiums, reverse=True)

            # Apply result limit if specified
            if self.max_results:
                best_calls = best_calls[:self.max_results]
                best_puts = best_puts[:self.max_results]

            # Return PUTs first, then CALLs
            return OrderedDict([
                ('PUT', best_puts),
                ('CALL', best_calls)
            ])

        except Exception as e:
            logger.error(f"Error in options screening: {e}")
            raise

    @staticmethod
    def _get_best_by_symbol(options: List[OptionMetrics]) -> List[OptionMetrics]:
        """Get the highest premium option for each symbol."""
        best_options = {}
        for option in options:
            if (option.symbol not in best_options or
                    option.premiums > best_options[option.symbol].premiums):
                best_options[option.symbol] = option
        return list(best_options.values())
