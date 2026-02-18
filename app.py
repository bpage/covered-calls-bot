from flask import Flask, jsonify, request, send_file
from datetime import datetime, timedelta
import os
import math

try:
    import yfinance as yf
except ImportError:
    yf = None

app = Flask(__name__)


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
        info = ticker.fast_info
        stock_price = float(info.last_price)

        expirations = ticker.options
        exp_timestamps = []
        all_options = []
        for exp_str in expirations:
            exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
            dte = (exp_date - today).days
            if dte < 20 or dte > 60:
                continue
            exp_ts = int(datetime.strptime(exp_str, "%Y-%m-%d").timestamp())
            exp_timestamps.append(exp_ts)

            chain = ticker.option_chain(exp_str)
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
        return jsonify({"error": str(e)}), 502


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(debug=debug, host="0.0.0.0", port=port)
