# Schwab Tracker

A Python tool for analyzing Schwab API data and options. Collects active stock data, fetches current prices and options chains, and analyzes option opportunities based on available funds.

## Installation

Clone the repository and install in development mode:

```bash
git clone <your-repository-url>
cd SchwabV2
pip install -e .
```

## Configuration

Edit `config/config.yml` to customize all settings:

### Required API Credentials
```yaml
api:
  alphavantage:
    key: "YOUR_ALPHAVANTAGE_API_KEY"  # From AlphaVantage
  schwab:
    app_key: "YOUR_SCHWAB_APP_KEY"      # From Schwab Developer Portal
    app_secret: "YOUR_SCHWAB_APP_SECRET" # From Schwab Developer Portal
    callback_url: "YOUR_CALLBACK_URL"    # Your OAuth callback URL
```

### Other Settings
- Database paths and names
- Logging preferences
- Options analysis parameters (minimum volume, price, etc.)
- API batch sizes and rate limits

## Usage

The workflow consists of three steps:

1. Get list of active stocks:
```bash
get-symbols
```

2. Collect current market data:
```bash
collect-data
```

3. Analyze options based on available funds:
```bash
analyze-options -f 25000 -r 10
```

Parameters for analyze-options:
- `-f, --funds`: Available funds for trading
- `-r, --results`: Number of top results to display (default: 10)

## Folder Structure

```
SchwabV2/
├── config/          # Configuration files
│   └── config.yml   # Central place for all settings
├── data/           # Data storage
│   ├── db/         # Database files
│   └── logs/       # Log files
├── src/            # Source code root
│   └── schwab_tracker/    # Main package
│       ├── analysis/      # Analysis and calculations
│       ├── api/           # API interaction code
│       ├── database/      # Database operations
│       ├── scripts/       # Command-line entry points
│       └── utils/         # Shared utilities
├── tests/          # Test files
├── setup.py        # Package installation and dependencies
└── README.md       # Project documentation
```

## Output Format

The analysis output shows:
1. PUT options first, followed by CALL options
2. For each option:
   - Symbol
   - Expiration date
   - Option type (PUT/CALL)
   - Strike price
   - Number of contracts possible with funds
   - Premium (potential income)
   - Exercise value

Options shown are filtered for:
- Expiring next Friday
- Out-of-the-money only
- Stocks with sufficient volume
- Prices above $5