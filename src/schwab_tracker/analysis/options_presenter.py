from typing import List, Dict
from dataclasses import dataclass
from decimal import Decimal
import logging
from datetime import datetime
from .options_analyzer import OptionMetrics, CoveredCallMetrics

logger = logging.getLogger(__name__)


@dataclass
class TableFormat:
    headers: List[str]
    widths: List[int]
    alignments: List[str]  # '<' for left, '>' for right


# Updated OptionsPresenter class for options_presenter.py

class OptionsPresenter:
    PUT_FORMAT = TableFormat(
        headers=['Symbol', 'Expiration', 'Type', 'Strike', 'Contracts', 'Premium'],
        widths=[10, 12, 6, 10, 11, 12],
        alignments=['<', '<', '<', '>', '>', '>']
    )

    CALL_FORMAT = TableFormat(
        headers=['Symbol', 'Expiration', 'Type', 'Strike', 'Contracts', 'Premium', 'Exercise'],
        widths=[10, 12, 6, 10, 11, 12, 12],
        alignments=['<', '<', '<', '>', '>', '>', '>']
    )

    def __init__(self):
        self.put_format = self.PUT_FORMAT
        self.call_format = self.CALL_FORMAT

    def format_options_table(self, options_data: Dict[str, List[OptionMetrics]], command: str = None) -> str:
        """Format options data into a readable table string."""
        output = []

        # Only show PUT options if 'puts' command is used
        if command == 'puts' and 'PUT' in options_data:
            output.append("\nPUT Options:")
            output.append(self._format_header('PUT'))
            output.append(self._format_separator('PUT'))
            for option in options_data['PUT']:
                output.append(self._format_option_row(option, 'PUT'))

        # Only show CALL options if 'calls' command is used
        elif command == 'calls' and 'CALL' in options_data:
            output.append("\nCALL Options:")
            output.append(self._format_header('CALL'))
            output.append(self._format_separator('CALL'))
            for option in options_data['CALL']:
                output.append(self._format_option_row(option, 'CALL'))

        return "\n".join(output)

    def _format_header(self, option_type: str) -> str:
        """Create the table header."""
        format_obj = self.put_format if option_type == 'PUT' else self.call_format
        return "".join(
            f"{header:{align}{width}}"
            for header, width, align in zip(
                format_obj.headers,
                format_obj.widths,
                format_obj.alignments
            )
        )

    def _format_separator(self, option_type: str) -> str:
        """Create the separator line."""
        format_obj = self.put_format if option_type == 'PUT' else self.call_format
        return "-" * sum(format_obj.widths)

    def _format_option_row(self, option: OptionMetrics, option_type: str) -> str:
        """Format a single option row."""
        if option_type == 'PUT':
            return "".join([
                f"{option.symbol:<{self.put_format.widths[0]}}",
                f"{option.expiration[:10]:<{self.put_format.widths[1]}}",
                f"{option.option_type:<{self.put_format.widths[2]}}",
                f"{option.strike:>{self.put_format.widths[3]}.2f}",
                f"{option.contracts:>{self.put_format.widths[4]}}",
                f"{option.premiums:>{self.put_format.widths[5]}.0f}"
            ])
        else:
            return "".join([
                f"{option.symbol:<{self.call_format.widths[0]}}",
                f"{option.expiration[:10]:<{self.call_format.widths[1]}}",
                f"{option.option_type:<{self.call_format.widths[2]}}",
                f"{option.strike:>{self.call_format.widths[3]}.2f}",
                f"{option.contracts:>{self.call_format.widths[4]}}",
                f"{option.premiums:>{self.call_format.widths[5]}.0f}",
                f"{option.exercise:>{self.call_format.widths[6]}.0f}"
            ])

class CoveredCallPresenter:
    DEFAULT_FORMAT = TableFormat(
        headers=['Expiry', 'Strike', 'Bid', 'Days', 'Delta', 'Theta', 'Annual %', 'Called %'],
        widths=[12, 8, 8, 6, 8, 8, 10, 10],
        alignments=['<', '>', '>', '>', '>', '>', '>', '>']
    )

    def __init__(self, table_format: TableFormat = None):
        self.format = table_format or self.DEFAULT_FORMAT

    def format_covered_calls_table(self, options: List[CoveredCallMetrics]) -> str:
        """Format covered call options into a readable table."""
        output = []

        if not options:
            return "No valid covered call options found."

        output.append("\nCovered Call Options Analysis")
        output.append("-" * 40)
        output.append(f"Symbol: {options[0].symbol}")
        output.append(self._format_header())
        output.append(self._format_separator())

        for option in options:
            output.append(self._format_option_row(option))

        return "\n".join(output)

    def _format_header(self) -> str:
        """Create the table header."""
        return "".join(
            f"{header:{align}{width}}"
            for header, width, align in zip(
                self.format.headers,
                self.format.widths,
                self.format.alignments
            )
        )

    def _format_separator(self) -> str:
        """Create the separator line."""
        return "-" * sum(self.format.widths)

    def _format_option_row(self, option: CoveredCallMetrics) -> str:
        """Format a single covered call option row."""
        return "".join([
            f"{option.expiration[:10]:<{self.format.widths[0]}}",
            f"{option.strike:>{self.format.widths[1]}.2f}",
            f"{option.premiums / 100:>{self.format.widths[2]}.2f}",
            f"{option.days_to_expiry:>{self.format.widths[3]}}",
            f"{option.delta:>{self.format.widths[4]}.3f}",
            f"{option.theta:>{self.format.widths[5]}.3f}",
            f"{option.annual_return:>{self.format.widths[6]}.1f}",
            f"{option.roi_if_called:>{self.format.widths[7]}.1f}"
        ])


def create_options_report(funds: Decimal, screener, presenter, command: str = None) -> str:
    """Generate a complete options analysis report."""
    try:
        best_options = screener.find_best_options(funds)
        formatted_table = presenter.format_options_table(best_options, command)

        return f"""
Options Analysis Report
----------------------
Available Funds: ${funds:,.2f}
Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

{formatted_table}
"""
    except Exception as e:
        logger.error(f"Error generating options report: {e}")
        raise


def create_covered_calls_report(symbol: str, analyzer) -> str:
    """Generate a complete covered calls analysis report."""
    try:
        options_data = analyzer.get_best_covered_calls(symbol)
        if not options_data:
            return f"No valid covered call options found for {symbol}"

        metrics = []
        for opt in options_data:
            # Calculate annual return (premium / days * 365)
            premium = Decimal(str(opt['bid'])) * 100
            days = opt['daysToExpiration']
            annual_return = (premium / days * 365) / (Decimal(str(opt['strikePrice'])) * 100) * 100

            metrics.append(CoveredCallMetrics(
                symbol=opt['symbol'],
                expiration=opt['expirationDate'],
                option_type='CALL',
                strike=Decimal(str(opt['strikePrice'])),
                contracts=1,  # Assuming 1 contract per analysis
                premiums=premium,
                exercise=Decimal(str(opt['strikePrice'])) * 100,
                delta=opt['delta'],
                theta=opt['theta'],
                annual_return=annual_return,
                days_to_expiry=days
            ))

        presenter = CoveredCallPresenter()
        return presenter.format_covered_calls_table(metrics)

    except Exception as e:
        logger.error(f"Error generating covered calls report: {e}")
        raise
