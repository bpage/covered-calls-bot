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


@app.route("/api/status")
def status():
    return jsonify({"status": "ok"})


@app.route("/api/yahoo/options/<symbol>")
def api_yahoo_options(symbol):
    """Fetch stock price + options data from Yahoo Finance via yfinance."""
    symbol = symbol.upper()

    try:
        ticker = yf.Ticker(symbol)

        # Get stock price
        info = ticker.fast_info
        stock_price = getattr(info, "last_price", None)
        if not stock_price:
            stock_price = getattr(info, "previous_close", None)
        if not stock_price:
            # Fallback: use history
            hist = ticker.history(period="1d")
            if not hist.empty:
                stock_price = float(hist["Close"].iloc[-1])
        if not stock_price:
            return jsonify({"error": f"No price data for {symbol}"}), 404

        stock_price = float(stock_price)
        logger.info(f"{symbol} price: ${stock_price:.2f}")

        # Get expiration dates
        try:
            exp_dates = ticker.options  # list of date strings like "2026-03-20"
        except Exception as e:
            logger.warning(f"No options available for {symbol}: {e}")
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

        if not exp_dates:
            logger.info(f"{symbol}: no expiration dates available")
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

        # Filter to 30-90 DTE
        now = datetime.now()
        valid_exps = []
        for exp_str in exp_dates:
            exp_date = datetime.strptime(exp_str, "%Y-%m-%d")
            dte = (exp_date - now).days
            if 30 <= dte <= 90:
                valid_exps.append(exp_str)

        logger.info(f"{symbol}: {len(valid_exps)} expirations in 5-90 DTE range out of {len(exp_dates)} total")

        # Fetch option chains for each valid expiration
        all_options = []
        for exp_str in valid_exps[:8]:
            try:
                chain = ticker.option_chain(exp_str)
                calls = chain.calls

                # Convert expiration string to unix timestamp
                exp_dt = datetime.strptime(exp_str, "%Y-%m-%d")
                exp_ts = int(exp_dt.timestamp())

                # Build calls list in Yahoo-compatible format
                calls_list = []
                for _, row in calls.iterrows():
                    call = {
                        "strike": float(row.get("strike", 0)),
                        "expiration": exp_ts,
                        "bid": float(row.get("bid", 0)),
                        "ask": float(row.get("ask", 0)),
                        "openInterest": int(row.get("openInterest", 0)) if not _is_nan(row.get("openInterest")) else 0,
                        "volume": int(row.get("volume", 0)) if not _is_nan(row.get("volume")) else 0,
                        "impliedVolatility": float(row.get("impliedVolatility", 0)),
                    }
                    calls_list.append(call)

                all_options.append({
                    "expirationDate": exp_ts,
                    "calls": calls_list,
                })
                logger.info(f"{symbol}: exp {exp_str} returned {len(calls_list)} calls")
            except Exception as e:
                logger.warning(f"Failed fetching {symbol} options for {exp_str}: {e}")

        # Convert valid expiration strings to timestamps for response
        exp_timestamps = []
        for exp_str in valid_exps:
            exp_dt = datetime.strptime(exp_str, "%Y-%m-%d")
            exp_timestamps.append(int(exp_dt.timestamp()))

        total_calls = sum(len(o.get("calls", [])) for o in all_options)
        logger.info(f"{symbol}: returning {len(all_options)} option chains with {total_calls} total calls")

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
