import argparse
from decimal import Decimal
import logging
import yaml
from pathlib import Path
from typing import Dict

from schwab_tracker.utils.logging_config import setup_logging
from schwab_tracker.database.operations import DatabaseManager
from schwab_tracker.analysis.options_analyzer import OptionsAnalyzer, OptionsScreener
from schwab_tracker.analysis.options_presenter import OptionsPresenter, create_options_report

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
    parser.add_argument(
        "-f", "--funds",
        type=float,
        required=True,
        help="Available funds for trading"
    )
    parser.add_argument(
        "-r", "--results",
        type=int,
        default=10,
        help="Number of top results to display (default: 10)"
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
        analyzer = OptionsAnalyzer(db_manager)
        screener = OptionsScreener(analyzer)
        presenter = OptionsPresenter()

        # Generate and display report
        funds = Decimal(str(args.funds))
        screener.max_results = args.results  # Set the max results
        report = create_options_report(funds, screener, presenter)
        print(report)

    except Exception as e:
        logging.error(f"Error in options analysis: {e}")
        raise

if __name__ == "__main__":
    main()