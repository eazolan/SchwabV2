# Schwab Tracker

A Python tool for analyzing Schwab API data and options. Collects active stock data, fetches current prices and options chains, and analyzes option opportunities based on available funds.

## Discord Bot Integration

These scripts can be automated and controlled remotely using [ScriptRunnerTodd](https://github.com/eazolan/ScriptRunnerTodd), a Discord bot specifically designed to work with this project. The bot allows you to:
- Trigger analysis scripts from Discord
- Receive results directly in your Discord channel
- Monitor script execution status

## Installation

Clone the repository and install in development mode:

```bash
git clone https://github.com/eazolan/SchwabV2
cd SchwabV2
pip install setuptools
pip install -e .
```

## Creating an Executable

You can create a standalone executable of this application using PyInstaller within a virtual environment:

1. Set up a virtual environment (if not already done):
```bash
python -m venv .venv
```

2. Activate the virtual environment:
```bash
# On Windows
.venv\Scripts\activate
```
```bash
# On Unix/MacOS
source .venv/bin/activate
```

3. Install required dependencies:
```bash
pip install -e .
pip install pyinstaller
```
4. Create the executable:
```bash
pyinstaller --onefile src/schwab_tracker/scripts/analyze_options.py --name analyze-options
pyinstaller --onefile src/schwab_tracker/scripts/collect_data.py --name collect-data
pyinstaller --onefile src/schwab_tracker/scripts/get_symbols.py --name get-symbols
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

1. Get list of active stocks and index memberships:
```bash
get-symbols
```
This command:
- Fetches all active stocks from AlphaVantage and stores them in `ActiveStocks.db`
- Fetches current S&P 500 constituents from GitHub and stores them in `BasicStockData.db`
- Tracks index membership history (additions and removals)

2. Collect current market data:
```bash
collect-data
```

3. Analyze options:

For PUT options based on available funds:
```bash
analyze-options puts -f 25000 -r 5
```

For PUT options expiring on a specific date:
```bash
analyze-options puts -f 25000 -d 2025-04-25 -r 20
```

For covered call opportunities on a specific stock:
```bash
analyze-options calls SYMBOL
```

For most volatile CALL options on a specific date:
```bash
analyze-options volatility -d 2026-05-01 -r 12
```

For most volatile CALL options filtered by index (e.g., S&P 500 only):
```bash
analyze-options volatility -d 2026-05-01 -i SP500 -r 25
```

Parameters for PUT analysis:
- `-f, --funds`: Available funds for trading
- `-r, --results`: Number of top results to display (default: 10)
- `-d, --date`: Specify expiration date in YYYY-MM-DD format (default: next Friday). Useful for holidays when exchanges are closed on Friday (e.g., Good Friday).
- `--include-nonstandard`: Include non-standard options (adjusted for splits/mergers). By default, these are filtered out.

Parameters for covered calls analysis:
- `SYMBOL`: Stock symbol to analyze
- `--include-nonstandard`: Include non-standard options (adjusted for splits/mergers)

Parameters for volatility analysis:
- `-d, --date`: Expiration date in YYYY-MM-DD format (required)
- `-r, --results`: Number of top results to display (default: 10)
- `-i, --index`: Filter by index membership (e.g., SP500). Optional - omit to include all stocks.
- `--include-nonstandard`: Include non-standard options (adjusted for splits/mergers)

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

```sql
PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE all_active_stocks (
                symbol TEXT PRIMARY KEY,
                assetType TEXT 
            );
COMMIT;
```

## Index Membership Tracking

The `get-symbols` command also creates and maintains an `index_memberships` table in `BasicStockData.db` that tracks which stocks belong to market indexes like the S&P 500.

### SQL for the `index_memberships` table in `BasicStockData.db`

```sql
CREATE TABLE index_memberships (
    symbol TEXT NOT NULL,
    index_name TEXT NOT NULL,
    added_date DATE,
    removed_date DATE,
    PRIMARY KEY (symbol, index_name, added_date)
);
```

The table tracks:
- `symbol`: Stock ticker symbol
- `index_name`: Name of the index (e.g., 'SP500')
- `added_date`: Date the stock was added to the index
- `removed_date`: Date the stock was removed (NULL if still active)

### Querying Index Membership

```sql
-- Get all current S&P 500 stocks
SELECT symbol FROM index_memberships 
WHERE index_name = 'SP500' AND removed_date IS NULL;

-- Check if a specific stock is in S&P 500
SELECT * FROM index_memberships 
WHERE symbol = 'AAPL' AND index_name = 'SP500' AND removed_date IS NULL;
```

## Output Format

### PUT Options Analysis
Shows:
- Symbol
- Expiration date
- Strike price
- Number of contracts possible with funds
- Premium (potential income)
- Exercise value

Options are filtered for:
- Expiration date (Default next Friday, or user specified.)
- Out-of-the-money only
- Stocks with sufficient volume
- Prices above $5
- Standard options only (unless --include-nonstandard is specified)

### Covered Calls Analysis
Shows for a single stock:
- All expiration dates within 90 days
- Options at the highest strike price below current stock price
- Bid price
- Days to expiration
- Delta and Theta Greeks
- Annualized return percentage
- Return if called percentage

### Volatility Analysis
Shows the most volatile CALL options for a specific expiration date:
- Symbol
- Strike price (lowest OTM strike per symbol)
- Volatility (implied volatility)
- Greeks (Delta, Theta, Gamma)
- Bid/Ask prices
- Underlying stock price
- Open Interest and Volume

Options are filtered for:
- Specific expiration date (user specified)
- Out-of-the-money only (strike > underlying price)
- Stocks with volume > 1 million
- Underlying price > $5
- One result per symbol (lowest strike price)
- Standard options only (unless --include-nonstandard is specified)
- Optional index filter (e.g., -i SP500 for S&P 500 stocks only)
