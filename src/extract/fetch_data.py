import requests
import yfinance as yf
import os, io

# ---------------- Alpha Vantage API ----------------

API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

def fetch_alphavantage_data(symbol, function):
    """
    :param symbol: Stock ticker (e.g., SPY)
    :param function: API function ('TIME_SERIES_DAILY', 'TIME_SERIES_WEEKLY', 'TIME_SERIES_MONTHLY')
    :return: CSV data as string
    """
    url = f"https://www.alphavantage.co/query"

    params = {
        "function": function,
        "symbol": symbol,
        "apikey": API_KEY,
        "datatype": "csv"  # Request CSV format
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:
        return response.text  # CSV data as string
    else:
        print(f"Error fetching {symbol} ({function}): {response.status_code}, {response.text}")
        return None


# ---------------- yfinance API ----------------
def fetch_yfinance_data(symbol, period):
    """
    :param symbol: Stock ticker
    :param period: Data period ('1d' for daily, '1wk' for weekly, '1mo' for monthly)
    :return: CSV data as string
    """
    try:
        stock = yf.Ticker(symbol)
        data = stock.history(period=period)

        if data.empty:
            print(f"No data found for {symbol} ({period})")
            return None

        # Convert DataFrame to CSV string
        csv_buffer = io.StringIO()
        data.to_csv(csv_buffer)
        return csv_buffer.getvalue()

    except Exception as e:
        print(f"Error fetching {symbol} ({period}): {str(e)}")
        return None
