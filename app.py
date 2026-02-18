from flask import Flask, jsonify, request, send_file
from datetime import datetime, timedelta
import os
import math
import logging
import traceback

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    import yfinance as yf
except ImportError:
    yf = None

app = Flask(__name__)


def get_stock_price(ticker):
    """Try multiple methods to get a reliable stock price."""
    errors = []

    # Method 1: fast_info.last_price
    try:
        price = float(ticker.fast_info.last_price)
        if price > 0:
            logger.info(f"Got price via fast_info: {price}")
            return price
    except Exception as e:
        errors.append(f"fast_info: {e}")

    # Method 2: info dict
    try:
        info = ticker.info
        for key in ["regularMarketPrice", "currentPrice", "previousClose"]:
            if key in info and info[key]:
                price = float(info[key])
                if price > 0:
                    logger.info(f"Got price via info[{key}]: {price}")
                    return price
    except Exception as e:
        errors.append(f"info: {e}")

    # Method 3: last close from history
    try:
        hist = ticker.history(period="5d")
        if not hist.empty:
            price = float(hist["Close"].iloc[-1])
            if price > 0:
                logger.info(f"Got price via history: {price}")
                return price
    except Exception as e:
        errors.append(f"history: {e}")

    raise ValueError(f"Could not get price. Tried: {'; '.join(errors)}")


@app.route("/")
def index():
    return send_file("index.html")


@app.route("/api/status")
def status():
    return jsonify({"yfinance": yf is not None})


@app.route("/api/yahoo/options/<symbol>")
def yahoo_options(symbol):
    """Fetch options data from Yahoo Finance."""
    symbol = symbol.upper()
    today = datetime.now().date()

    if not yf:
        return jsonify({"error": "yfinance not installed"}), 502

    try:
        ticker = yf.Ticker(symbol)
        stock_price = get_stock_price(ticker)
        logger.info(f"{symbol} price: {stock_price}")

        try:
            expirations = ticker.options
        except Exception as e:
            logger.error(f"Failed to get options expirations for {symbol}: {e}")
            # Return price even if options fail
            return jsonify({
                "source": "yfinance",
                "optionChain": {
                    "result": [{
                        "quote": {"regularMarketPrice": stock_price},
                        "expirationDates": [],
                        "options": [],
                    }]
                }
            })

        exp_timestamps = []
        all_options = []
        for exp_str in expirations:
            exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
            dte = (exp_date - today).days
            if dte < 20 or dte > 60:
                continue
            exp_ts = int(datetime.strptime(exp_str, "%Y-%m-%d").timestamp())
            exp_timestamps.append(exp_ts)

            try:
                chain = ticker.option_chain(exp_str)
            except Exception as e:
                logger.warning(f"Failed to get chain for {exp_str}: {e}")
                continue

            call_list = []
            for _, row in chain.calls.iterrows():
                call_list.append({
                    "strike": float(row["strike"]),
                    "bid": float(row.get("bid", 0) or 0),
                    "ask": float(row.get("ask", 0) or 0),
                    "volume": int(row.get("volume", 0) or 0),
                    "openInterest": int(row.get("openInterest", 0) or 0),
                    "impliedVolatility": float(row.get("impliedVolatility", 0) or 0),
                    "expiration": exp_ts,
                })
            all_options.append({"expirationDate": exp_ts, "calls": call_list})

        result = {
            "source": "yfinance",
            "optionChain": {
                "result": [{
                    "quote": {"regularMarketPrice": stock_price},
                    "expirationDates": exp_timestamps,
                    "options": all_options,
                }]
            }
        }
        return jsonify(result)
    except Exception as e:
        logger.error(f"Yahoo options error for {symbol}: {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e)}), 502


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(debug=debug, host="0.0.0.0", port=port)
