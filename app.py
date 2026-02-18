from flask import Flask, jsonify, request, send_file
from datetime import datetime, timedelta
import math

try:
    import robin_stocks.robinhood as r
except ImportError:
    r = None

try:
    import yfinance as yf
except ImportError:
    yf = None

app = Flask(__name__)


@app.route("/")
def index():
    return send_file("index.html")


@app.route("/api/login", methods=["POST"])
def login():
    data = request.json
    email = data.get("email", "")
    password = data.get("password", "")
    mfa_code = data.get("mfa_code")

    try:
        kwargs = {}
        if mfa_code:
            kwargs["mfa_code"] = mfa_code
        r.login(email, password, **kwargs)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 401


@app.route("/api/quote/<symbol>")
def get_quote(symbol):
    try:
        price_list = r.stocks.get_latest_price(symbol)
        stock_price = float(price_list[0]) if price_list and price_list[0] else None
        return jsonify({"success": True, "price": stock_price})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/options/<symbol>")
def get_options(symbol):
    min_dte = int(request.args.get("min_dte", 30))
    max_dte = int(request.args.get("max_dte", 45))
    min_delta = float(request.args.get("min_delta", 0.10))
    max_delta = float(request.args.get("max_delta", 0.30))
    option_type = request.args.get("type", "call")

    try:
        # Get current stock price
        price_list = r.stocks.get_latest_price(symbol)
        stock_price = float(price_list[0]) if price_list and price_list[0] else None

        # Get option chain expiration dates
        chains = r.options.get_chains(symbol)
        if not chains:
            return jsonify({"success": False, "error": "No option chain found"}), 404

        expiration_dates = chains.get("expiration_dates", [])

        # Filter to desired DTE range
        today = datetime.now().date()
        target_min = today + timedelta(days=min_dte)
        target_max = today + timedelta(days=max_dte)

        valid_dates = []
        for d in expiration_dates:
            exp_date = datetime.strptime(d, "%Y-%m-%d").date()
            if target_min <= exp_date <= target_max:
                valid_dates.append(d)

        options = []
        for exp_date in valid_dates:
            # Find all tradable options for this expiration
            opts = r.options.find_options_by_expiration(
                symbol, exp_date, optionType=option_type
            )
            if not opts:
                continue

            for opt in opts:
                strike = opt.get("strike_price")
                if not strike:
                    continue

                try:
                    market_data = r.options.get_option_market_data(
                        symbol, exp_date, strike, option_type
                    )
                    if not market_data:
                        continue

                    # market_data returns nested lists sometimes
                    md = market_data[0]
                    if isinstance(md, list):
                        md = md[0] if md else {}

                    delta_val = md.get("delta")
                    if delta_val is None:
                        continue
                    delta_val = abs(float(delta_val))

                    # Filter by delta range
                    if min_delta <= delta_val <= max_delta:
                        dte = (
                            datetime.strptime(exp_date, "%Y-%m-%d").date() - today
                        ).days
                        bid = float(md.get("bid_price", 0) or 0)
                        ask = float(md.get("ask_price", 0) or 0)
                        oi = int(float(md.get("open_interest", 0) or 0))
                        vol = int(float(md.get("volume", 0) or 0))

                        options.append(
                            {
                                "strike": float(strike),
                                "exp": exp_date,
                                "dte": dte,
                                "delta": round(delta_val, 4),
                                "bid": bid,
                                "ask": ask,
                                "oi": oi,
                                "vol": vol,
                            }
                        )
                except Exception:
                    continue

        options.sort(key=lambda x: (x["exp"], x["strike"]))

        return jsonify(
            {"success": True, "stock_price": stock_price, "options": options}
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/yahoo/options/<symbol>")
def yahoo_options(symbol):
    """Fetch options data via yfinance library."""
    if not yf:
        return jsonify({"error": "yfinance not installed"}), 502
    try:
        ticker = yf.Ticker(symbol.upper())
        info = ticker.fast_info
        stock_price = float(info.last_price)

        expirations = ticker.options  # list of date strings
        today = datetime.now().date()
        now_ts = datetime.now().timestamp()

        # Build response matching the format the frontend expects
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
            calls = chain.calls
            call_list = []
            for _, row in calls.iterrows():
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


@app.route("/api/logout", methods=["POST"])
def logout():
    try:
        if r:
            r.logout()
    except Exception:
        pass
    return jsonify({"success": True})


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(debug=debug, host="0.0.0.0", port=port)
