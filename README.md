# Schwab Tracker

A Python tool for analyzing Schwab API data and options. Collects active stock data, fetches current prices and options chains, and analyzes option opportunities based on available funds.

## Installation

Clone the repository and install in development mode:

```bash
git clone https://github.com/eazolan/SchwabV2
cd SchwabV2
pip install -e .
```

## Configuration

You must have a "Market Data Production" API product from Charles-Schwab. 
Put in 127.0.0.1 as the "Callback URL" in both the API setup on Charles-Schwab, and the yaml file.

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
### First time running

The Alphavantage should just work with the API key you've gotten from them.

Schwab is a bit more complex. You need to set up an app on the "Apps Dashboard" of their website `https://developer.schwab.com/`, using the "Market Data Production" API product. That level of security only allows you to view data, no trading. Then it will take a week for them to approve it. Then you'll have the app_key and app_secret, to update the config.yml.

After that, running `collect-data` for the first time will open up a Schwab login web page. You'll need to log in using your trading account login, not your developer account login. It will ask you for approval, and the account this is linked to. 

After that it will try to open up a 127.0.0.1 URL with a key. It will fail. Grab the full URL and paste it into CMD window that you're running `collect-data` out of. 

You'll be good to go after that. You should not need to jump through these hoops again for a long time.

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

## What if I don't want to use Alphavantage?

Right now this is just a way to get a list of active stocks. It creates an sqlite3 database, with one table. `ActiveStocks.db` in data/db. You are free to create this sqlite3 DB and populate it any way you wish. As of version 0.9 The `assetType` column data isn't currently refered to.

### SQL for creating the `all_active_stocks` table in `ActiveStocks.db`

```
PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE all_active_stocks (
                symbol TEXT PRIMARY KEY,
                assetType TEXT 
            );
COMMIT;
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


