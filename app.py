from flask import Flask, jsonify, request, send_file
from datetime import datetime
import os
import logging
import traceback
import yfinance as yf

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.route("/")
def index():
    return send_file("index.html")


@app.route("/covered-calls")
def covered_calls():
    return send_file("covered-calls.html")


@app.route("/cash-secured-puts")
def cash_secured_puts():
    return send_file("cash-secured-puts.html")


@app.route("/api/status")
def status():
    return jsonify({"status": "ok"})


def _get_stock_price(ticker, symbol):
    """Get stock price from yfinance ticker object."""
    info = ticker.fast_info
    stock_price = getattr(info, "last_price", None)
    if not stock_price:
        stock_price = getattr(info, "previous_close", None)
    if not stock_price:
        hist = ticker.history(period="1d")
        if not hist.empty:
            stock_price = float(hist["Close"].iloc[-1])
    return float(stock_price) if stock_price else None


def _get_valid_expirations(ticker, symbol):
    """Get expiration dates filtered to 25-90 DTE."""
    try:
        exp_dates = ticker.options
    except Exception as e:
        logger.warning(f"No options available for {symbol}: {e}")
        return [], []

    if not exp_dates:
        return [], []

    now = datetime.now()
    valid_exps = []
    for exp_str in exp_dates:
        exp_date = datetime.strptime(exp_str, "%Y-%m-%d")
        dte = (exp_date - now).days
        if 25 <= dte <= 90:
            valid_exps.append(exp_str)

    logger.info(f"{symbol}: {len(valid_exps)} expirations in 25-90 DTE range out of {len(exp_dates)} total")
    return exp_dates, valid_exps


def _empty_response(stock_price):
    """Return empty options response."""
    return jsonify({
        "source": "yfinance",
        "optionChain": {
            "result": [{
                "quote": {"regularMarketPrice": stock_price or 0},
                "expirationDates": [],
                "options": [],
            }]
        }
    })


def _build_option_list(df, exp_ts):
    """Convert a yfinance options DataFrame to a list of dicts."""
    options_list = []
    for _, row in df.iterrows():
        opt = {
            "strike": float(row.get("strike", 0)),
            "expiration": exp_ts,
            "bid": float(row.get("bid", 0)),
            "ask": float(row.get("ask", 0)),
            "openInterest": int(row.get("openInterest", 0)) if not _is_nan(row.get("openInterest")) else 0,
            "volume": int(row.get("volume", 0)) if not _is_nan(row.get("volume")) else 0,
            "impliedVolatility": float(row.get("impliedVolatility", 0)),
        }
        options_list.append(opt)
    return options_list


@app.route("/api/yahoo/options/<symbol>")
def api_yahoo_options(symbol):
    """Fetch stock price + options data from Yahoo Finance via yfinance."""
    symbol = symbol.upper()

    try:
        ticker = yf.Ticker(symbol)
        stock_price = _get_stock_price(ticker, symbol)
        if not stock_price:
            return jsonify({"error": f"No price data for {symbol}"}), 404

        logger.info(f"{symbol} price: ${stock_price:.2f}")

        exp_dates, valid_exps = _get_valid_expirations(ticker, symbol)
        if not valid_exps:
            return _empty_response(stock_price)

        # Fetch option chains for each valid expiration
        all_options = []
        for exp_str in valid_exps[:8]:
            try:
                chain = ticker.option_chain(exp_str)
                exp_dt = datetime.strptime(exp_str, "%Y-%m-%d")
                exp_ts = int(exp_dt.timestamp())

                calls_list = _build_option_list(chain.calls, exp_ts)
                puts_list = _build_option_list(chain.puts, exp_ts)

                all_options.append({
                    "expirationDate": exp_ts,
                    "calls": calls_list,
                    "puts": puts_list,
                })
                logger.info(f"{symbol}: exp {exp_str} returned {len(calls_list)} calls, {len(puts_list)} puts")
            except Exception as e:
                logger.warning(f"Failed fetching {symbol} options for {exp_str}: {e}")

        exp_timestamps = []
        for exp_str in valid_exps:
            exp_dt = datetime.strptime(exp_str, "%Y-%m-%d")
            exp_timestamps.append(int(exp_dt.timestamp()))

        total_calls = sum(len(o.get("calls", [])) for o in all_options)
        total_puts = sum(len(o.get("puts", [])) for o in all_options)
        logger.info(f"{symbol}: returning {len(all_options)} option chains with {total_calls} calls, {total_puts} puts")

        return jsonify({
            "source": "yfinance",
            "optionChain": {
                "result": [{
                    "quote": {"regularMarketPrice": stock_price},
                    "expirationDates": exp_timestamps,
                    "options": all_options,
                }]
            }
        })

    except Exception as e:
        logger.error(f"Failed for {symbol}: {e}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 502


def _is_nan(val):
    """Check if a value is NaN."""
    try:
        return val != val  # NaN != NaN is True
    except Exception:
        return False


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(debug=debug, host="0.0.0.0", port=port)
