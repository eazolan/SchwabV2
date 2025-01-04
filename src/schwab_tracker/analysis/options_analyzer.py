from typing import List, Dict, Any, NamedTuple
from decimal import Decimal, ROUND_DOWN
from dataclasses import dataclass
import logging
from collections import OrderedDict

logger = logging.getLogger(__name__)

@dataclass
class OptionMetrics:
    symbol: str
    expiration: str
    option_type: str
    strike: Decimal
    contracts: int
    premiums: Decimal
    exercise: Decimal
    
    @property
    def profit_potential(self) -> Decimal:
        """Calculate potential profit from the option."""
        return self.premiums + self.exercise

class OptionsAnalyzer:
    def __init__(self, db_manager):
        self.db = db_manager

    def get_otm_options(self) -> List[Dict[str, Any]]:
        """Fetch out-of-the-money options from database."""
        return self.db.execute_query("""
            SELECT 
                symbol, 
                expirationDate, 
                strikePrice, 
                bid, 
                putCall, 
                underlyingPrice
            FROM temp_options_table
            WHERE bid > 0
            AND (
                (putCall = 'CALL' AND strikePrice > underlyingPrice)
                OR (putCall = 'PUT' AND strikePrice < underlyingPrice)
            )
            ORDER BY symbol, expirationDate
        """)

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

# ... (previous imports and OptionsAnalyzer class stay the same)

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

    @staticmethod
    def _get_best_by_symbol(options: List[OptionMetrics]) -> List[OptionMetrics]:
        """Get the highest premium option for each symbol."""
        best_options = {}
        for option in options:
            if (option.symbol not in best_options or 
                option.premiums > best_options[option.symbol].premiums):
                best_options[option.symbol] = option
        return list(best_options.values())