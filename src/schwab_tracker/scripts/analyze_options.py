import argparse
from decimal import Decimal
import logging
import yaml
from pathlib import Path
from typing import Dict

from schwab_tracker.utils.logging_config import setup_logging
from schwab_tracker.database.operations import DatabaseManager
from schwab_tracker.analysis.options_analyzer import OptionsAnalyzer, OptionsScreener
from schwab_tracker.analysis.options_presenter import (
    OptionsPresenter,
    create_options_report,
    create_covered_calls_report,
    create_volatility_report
)


def load_config() -> Dict:
    """Load configuration from YAML file."""
    config_path = Path(__file__).resolve().parent.parent.parent.parent / 'config' / 'config.yml'
    with open(config_path) as f:
        return yaml.safe_load(f)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Analyze options based on available funds"
    )

    subparsers = parser.add_subparsers(dest='command', required=True, help='Command to run')

    # Put options analysis
    puts_parser = subparsers.add_parser('puts', help='Analyze put options')
    puts_parser.add_argument(
        "-f", "--funds",
        type=float,
        required=True,
        help="Available funds for trading"
    )
    puts_parser.add_argument(
        "-r", "--results",
        type=int,
        default=10,
        help="Number of top results to display (default: 10)"
    )
    puts_parser.add_argument(
        "-i", "--index",
        type=str,
        help="Filter by index (e.g., SP500)"
    )
    puts_parser.add_argument(
        "--include-nonstandard",
        action="store_true",
        help="Include non-standard options (adjusted for splits/mergers)"
    )
    puts_parser.add_argument(
        "-d", "--date",
        type=str,
        help="Specify expiration date in YYYY-MM-DD format (default: next Friday)"
    )

    # Covered calls analysis
    calls_parser = subparsers.add_parser('calls', help='Analyze covered calls')
    calls_parser.add_argument(
        "symbol",
        type=str,
        help="Stock symbol to analyze"
    )
    calls_parser.add_argument(
        "--include-nonstandard",
        action="store_true",
        help="Include non-standard options (adjusted for splits/mergers)"
    )

    # Volatility analysis
    volatility_parser = subparsers.add_parser('volatility', help='Analyze most volatile calls')
    volatility_parser.add_argument(
        "-d", "--date",
        type=str,
        required=True,
        help="Expiration date in YYYY-MM-DD format"
    )
    volatility_parser.add_argument(
        "-r", "--results",
        type=int,
        default=10,
        help="Number of results to display (default: 10)"
    )
    volatility_parser.add_argument(
        "-i", "--index",
        type=str,
        help="Filter by index (e.g., SP500)"
    )
    volatility_parser.add_argument(
        "--include-nonstandard",
        action="store_true",
        help="Include non-standard options (adjusted for splits/mergers)"
    )

    return parser.parse_args()


def main():
    """Main entry point for options analysis."""
    try:
        # Setup
        config = load_config()
        setup_logging(config)
        args = parse_arguments()

        # Initialize components
        db_manager = DatabaseManager(config)

        if args.command == 'puts':
            index_filter = args.index if hasattr(args, 'index') else None
            analyzer = OptionsAnalyzer(db_manager, include_nonstandard=args.include_nonstandard, 
                                       custom_date=args.date, index_filter=index_filter)
            screener = OptionsScreener(analyzer)
            screener.max_results = args.results
            presenter = OptionsPresenter()

            # Generate and display report
            funds = Decimal(str(args.funds))
            report = create_options_report(funds, screener, presenter, command='puts', index_filter=index_filter)
            print(report)

        elif args.command == 'calls':
            analyzer = OptionsAnalyzer(db_manager, include_nonstandard=args.include_nonstandard)
            symbol = args.symbol.upper()  # Capitalize the stock symbol
            report = create_covered_calls_report(symbol, analyzer)
            print(report)

        elif args.command == 'volatility':
            analyzer = OptionsAnalyzer(db_manager, include_nonstandard=args.include_nonstandard)
            index_filter = args.index if hasattr(args, 'index') else None
            report = create_volatility_report(args.date, analyzer, args.results, index_filter)
            print(report)

        else:
            print("Please specify a command: 'puts', 'calls', or 'volatility'")

    except Exception as e:
        logging.error(f"Error in options analysis: {e}")
        raise


if __name__ == "__main__":
    main()