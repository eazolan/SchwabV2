from typing import List, Dict
from dataclasses import dataclass
from decimal import Decimal
import logging
from datetime import datetime
from .options_analyzer import OptionMetrics

logger = logging.getLogger(__name__)

@dataclass
class TableFormat:
    headers: List[str]
    widths: List[int]
    alignments: List[str]  # '<' for left, '>' for right

class OptionsPresenter:
    DEFAULT_FORMAT = TableFormat(
        headers=['Symbol', 'Expiration', 'Type', 'Strike', 'Contracts', 'Premium', 'Exercise'],
        widths=[10, 12, 6, 10, 11, 12, 12],
        alignments=['<', '<', '<', '>', '>', '>', '>']
    )

    def __init__(self, table_format: TableFormat = None):
        self.format = table_format or self.DEFAULT_FORMAT

    def format_options_table(self, options_data: Dict[str, List[OptionMetrics]]) -> str:
        """Format options data into a readable table string."""
        output = []

        # Always process PUTs first, then CALLs
        option_types = ['PUT', 'CALL']
        for option_type in option_types:
            if option_type in options_data:
                output.append(f"\n{option_type} Options:")
                output.append(self._format_header())
                output.append(self._format_separator())
                
                for option in options_data[option_type]:
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

    def _format_option_row(self, option: OptionMetrics) -> str:
        """Format a single option row."""
        return "".join([
            f"{option.symbol:<{self.format.widths[0]}}",
            f"{option.expiration[:10]:<{self.format.widths[1]}}",
            f"{option.option_type:<{self.format.widths[2]}}",
            f"{option.strike:>{self.format.widths[3]}.2f}",
            f"{option.contracts:>{self.format.widths[4]}}",
            f"{option.premiums:>{self.format.widths[5]}.0f}",
            f"{option.exercise:>{self.format.widths[6]}.0f}"
        ])

def create_options_report(funds: Decimal, screener, presenter) -> str:
    """Generate a complete options analysis report."""
    try:
        best_options = screener.find_best_options(funds)
        formatted_table = presenter.format_options_table(best_options)
        
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