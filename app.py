from flask import Flask, jsonify, request, send_file
from datetime import datetime
import os
import logging
import traceback
import requests as http_requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Yahoo Finance direct HTTP — bypasses yfinance library entirely
YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

# Cache the session + crumb (reuse across requests)
_yahoo_session = None
_yahoo_crumb = None


def get_yahoo_session():
    """Get a Yahoo Finance session with cookie + crumb for API access."""
    global _yahoo_session, _yahoo_crumb

    if _yahoo_session and _yahoo_crumb:
        return _yahoo_session, _yahoo_crumb

    session = http_requests.Session()
    session.headers.update(YAHOO_HEADERS)

    # Step 1: Visit Yahoo Finance to get cookies
    try:
        resp = session.get("https://finance.yahoo.com/quote/AAPL", timeout=10)
        resp.raise_for_status()
        logger.info(f"Yahoo session cookies: {len(session.cookies)} cookies")
    except Exception as e:
        logger.warning(f"Failed to get Yahoo cookies: {e}")

    # Step 2: Get crumb using the cookies
    try:
        crumb_resp = session.get(
            "https://query2.finance.yahoo.com/v1/test/getcrumb",
            timeout=10
        )
        crumb_resp.raise_for_status()
        crumb = crumb_resp.text.strip()
        if crumb and len(crumb) < 50:
            logger.info(f"Got Yahoo crumb: {crumb[:8]}...")
            _yahoo_session = session
            _yahoo_crumb = crumb
            return session, crumb
    except Exception as e:
        logger.warning(f"Failed to get Yahoo crumb: {e}")

    # Return session without crumb — some endpoints work without it
    _yahoo_session = session
    _yahoo_crumb = ""
    return session, ""


def invalidate_yahoo_session():
    """Clear cached session so next request creates a fresh one."""
    global _yahoo_session, _yahoo_crumb
    _yahoo_session = None
    _yahoo_crumb = None


def yahoo_chart(symbol, session, crumb):
    """Get stock price from Yahoo Finance v8 chart API."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"range": "1d", "interval": "1d"}
    if crumb:
        params["crumb"] = crumb

    resp = session.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    result = data.get("chart", {}).get("result", [])
    if not result:
        raise ValueError(f"No chart data for {symbol}")

    meta = result[0].get("meta", {})
    price = meta.get("regularMarketPrice", 0)
    if not price:
        price = meta.get("previousClose", 0)
    if not price:
        raise ValueError(f"No price in chart data for {symbol}")

    return float(price)


def yahoo_options(symbol, session, crumb, date=None):
    """Get options chain from Yahoo Finance v7 options API."""
    url = f"https://query1.finance.yahoo.com/v7/finance/options/{symbol}"
    params = {}
    if date:
        params["date"] = date
    if crumb:
        params["crumb"] = crumb

    resp = session.get(url, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


@app.route("/")
def index():
    return send_file("index.html")


@app.route("/api/status")
def status():
    return jsonify({"status": "ok"})


@app.route("/api/yahoo/options/<symbol>")
def api_yahoo_options(symbol):
    """Fetch stock price + options data from Yahoo Finance via direct HTTP."""
    symbol = symbol.upper()
    today = datetime.now().date()

    # Try with cached session first, retry with fresh session on failure
    for attempt in range(2):
        try:
            session, crumb = get_yahoo_session()

            # Get stock price from chart API (most reliable)
            stock_price = yahoo_chart(symbol, session, crumb)
            logger.info(f"{symbol} price: ${stock_price:.2f} (attempt {attempt + 1})")

            # Get options chain
            try:
                options_data = yahoo_options(symbol, session, crumb)
            except Exception as e:
                logger.warning(f"Options fetch failed for {symbol}: {e}")
                # Return price even if options fail
                return jsonify({
                    "source": "yahoo_direct",
                    "optionChain": {
                        "result": [{
                            "quote": {"regularMarketPrice": stock_price},
                            "expirationDates": [],
                            "options": [],
                        }]
                    }
                })

            oc = options_data.get("optionChain", {})
            results = oc.get("result", [])
            if not results:
                return jsonify({
                    "source": "yahoo_direct",
                    "optionChain": {
                        "result": [{
                            "quote": {"regularMarketPrice": stock_price},
                            "expirationDates": [],
                            "options": [],
                        }]
                    }
                })

            result = results[0]
            # Override price from options response with chart price (more reliable)
            if result.get("quote"):
                result["quote"]["regularMarketPrice"] = stock_price
            else:
                result["quote"] = {"regularMarketPrice": stock_price}

            exp_dates = result.get("expirationDates", [])
            now_sec = datetime.now().timestamp()

            # Filter options in first response to 20-60 DTE
            first_options = result.get("options", [])

            # Fetch additional expiration dates in 20-60 DTE range
            valid_exps = [e for e in exp_dates if 20 <= (e - now_sec) / 86400 <= 60]
            first_exp = first_options[0].get("expirationDate") if first_options else None

            all_options = list(first_options)
            for exp_ts in valid_exps[:3]:
                if exp_ts == first_exp:
                    continue
                try:
                    more = yahoo_options(symbol, session, crumb, date=exp_ts)
                    more_result = more.get("optionChain", {}).get("result", [{}])[0]
                    more_opts = more_result.get("options", [])
                    all_options.extend(more_opts)
                except Exception as e:
                    logger.warning(f"Failed fetching expiration {exp_ts}: {e}")

            # Filter exp_timestamps to valid range
            exp_timestamps = [e for e in exp_dates if 20 <= (e - now_sec) / 86400 <= 60]

            return jsonify({
                "source": "yahoo_direct",
                "optionChain": {
                    "result": [{
                        "quote": {"regularMarketPrice": stock_price},
                        "expirationDates": exp_timestamps,
                        "options": all_options,
                    }]
                }
            })

        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed for {symbol}: {e}")
            if attempt == 0:
                # First failure — invalidate session and retry
                invalidate_yahoo_session()
                continue
            else:
                logger.error(traceback.format_exc())
                return jsonify({"error": str(e)}), 502

    return jsonify({"error": "All attempts failed"}), 502


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(debug=debug, host="0.0.0.0", port=port)
