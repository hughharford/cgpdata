import requests
import os
import yfinance as yf
import json

# ---------------- Alpha Vantage API ----------------

API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

def fetch_alphavan_data(symbol, function):
    """
    Fetches data from Alpha Vantage API for a given function.
    """
    url = f"https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={API_KEY}"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Error fetching {function} data: {response.status_code}")
        return None

    return response.json()

# ---------------- yfinance API ----------------
def fetch_yfinance_data(symbol, period):
    """
    Fetches stock data from Yahoo Finance.

    :param symbol: Stock ticker (e.g., SPY)
    :param period: Data period ('1d' for daily, '1wk' for weekly, '1mo' for monthly)
    :return: JSON data
    """
    try:
        stock = yf.Ticker(symbol)
        data = stock.history(period=period)

        if data.empty:
            print(f"No data found for {symbol} ({period})")
            return None

        return data.to_json()  # Convert DataFrame to JSON

    except Exception as e:
        print(f"Error fetching {symbol} ({period}): {str(e)}")
        return None
