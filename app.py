from flask import Flask, jsonify, request, send_file
from datetime import datetime, timedelta
import os
import logging
import traceback
import requests
import yfinance as yf

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ── MOMO INDEX blueprints ──
from momo_api import momo_bp
from reddit_scanner import reddit_bp
app.register_blueprint(momo_bp)
app.register_blueprint(reddit_bp)


@app.route("/momo")
def momo_dashboard():
    return send_file("momo-index-v3.html")

# Alpaca API credentials (set via environment variables)
ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "")
ALPACA_DATA_URL = "https://data.alpaca.markets"


@app.route("/")
def index():
    return send_file("index.html")


@app.route("/covered-calls")
def covered_calls():
    return send_file("covered-calls.html")


@app.route("/cash-secured-puts")
def cash_secured_puts():
    return send_file("cash-secured-puts.html")


@app.route("/iv-hunter")
def iv_hunter():
    return send_file("iv-hunter.html")


@app.route("/api/status")
def status():
    return jsonify({"status": "ok", "alpaca": bool(ALPACA_API_KEY)})


# ── Alpaca Data Fetching ──

def _alpaca_headers():
    return {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
    }


def _alpaca_stock_price(symbol):
    """Get latest stock price from Alpaca."""
    url = f"{ALPACA_DATA_URL}/v2/stocks/{symbol}/snapshot?feed=iex"
    resp = requests.get(url, headers=_alpaca_headers(), timeout=10)
    resp.raise_for_status()
    data = resp.json()
    # Use latest trade price, fallback to daily bar close
    price = None
    if data.get("latestTrade"):
        price = data["latestTrade"].get("p")
    if not price and data.get("dailyBar"):
        price = data["dailyBar"].get("c")
    return float(price) if price else None


def _alpaca_options_chain(symbol, opt_type, exp_gte, exp_lte):
    """Fetch all options snapshots from Alpaca with pagination."""
    all_snapshots = {}
    page_token = None

    for _ in range(10):  # max 10 pages
        params = {
            "feed": "indicative",
            "type": opt_type,
            "expiration_date_gte": exp_gte,
            "expiration_date_lte": exp_lte,
            "limit": 100,
        }
        if page_token:
            params["page_token"] = page_token

        url = f"{ALPACA_DATA_URL}/v1beta1/options/snapshots/{symbol}"
        resp = requests.get(url, headers=_alpaca_headers(), params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        snapshots = data.get("snapshots", {})
        all_snapshots.update(snapshots)

        page_token = data.get("next_page_token")
        if not page_token:
            break

    return all_snapshots


def _parse_occ_symbol(occ_symbol):
    """Parse OCC option symbol like NVDA260320C00205000.
    Returns (underlying, expiration_date_str, option_type, strike)."""
    # Find where the date starts (6 digits after the underlying)
    # Format: SYMBOL + YYMMDD + C/P + 00000000 (strike * 1000)
    # Find the type character (C or P) by scanning from the end
    for i in range(len(occ_symbol) - 9, 0, -1):
        if occ_symbol[i] in ('C', 'P'):
            underlying = occ_symbol[:i - 6]
            date_str = occ_symbol[i - 6:i]
            opt_type = occ_symbol[i]
            strike = int(occ_symbol[i + 1:]) / 1000
            exp_date = datetime.strptime(date_str, "%y%m%d").strftime("%Y-%m-%d")
            return underlying, exp_date, opt_type, strike
    return None, None, None, None


def _build_alpaca_response(symbol, stock_price, call_snapshots, put_snapshots):
    """Convert Alpaca snapshots into our standard response format."""
    # Group by expiration date
    expirations = {}

    for occ_sym, snap in call_snapshots.items():
        _, exp_date, _, strike = _parse_occ_symbol(occ_sym)
        if not exp_date:
            continue
        if exp_date not in expirations:
            expirations[exp_date] = {"calls": [], "puts": []}
        quote = snap.get("latestQuote", {})
        greeks = snap.get("greeks", {})
        bar = snap.get("dailyBar", {})
        expirations[exp_date]["calls"].append({
            "strike": strike,
            "expiration": int(datetime.strptime(exp_date, "%Y-%m-%d").timestamp()),
            "bid": quote.get("bp", 0) or 0,
            "ask": quote.get("ap", 0) or 0,
            "openInterest": 0,
            "volume": bar.get("v", 0) or 0,
            "impliedVolatility": snap.get("impliedVolatility", 0) or 0,
            "delta": greeks.get("delta", 0) or 0,
            "gamma": greeks.get("gamma", 0) or 0,
            "theta": greeks.get("theta", 0) or 0,
        })

    for occ_sym, snap in put_snapshots.items():
        _, exp_date, _, strike = _parse_occ_symbol(occ_sym)
        if not exp_date:
            continue
        if exp_date not in expirations:
            expirations[exp_date] = {"calls": [], "puts": []}
        quote = snap.get("latestQuote", {})
        greeks = snap.get("greeks", {})
        bar = snap.get("dailyBar", {})
        expirations[exp_date]["puts"].append({
            "strike": strike,
            "expiration": int(datetime.strptime(exp_date, "%Y-%m-%d").timestamp()),
            "bid": quote.get("bp", 0) or 0,
            "ask": quote.get("ap", 0) or 0,
            "openInterest": 0,
            "volume": bar.get("v", 0) or 0,
            "impliedVolatility": snap.get("impliedVolatility", 0) or 0,
            "delta": greeks.get("delta", 0) or 0,
            "gamma": greeks.get("gamma", 0) or 0,
            "theta": greeks.get("theta", 0) or 0,
        })

    # Build options array sorted by expiration
    all_options = []
    exp_timestamps = []
    for exp_date in sorted(expirations.keys()):
        exp_ts = int(datetime.strptime(exp_date, "%Y-%m-%d").timestamp())
        exp_timestamps.append(exp_ts)
        all_options.append({
            "expirationDate": exp_ts,
            "calls": sorted(expirations[exp_date]["calls"], key=lambda x: x["strike"]),
            "puts": sorted(expirations[exp_date]["puts"], key=lambda x: x["strike"]),
        })

    total_calls = sum(len(o["calls"]) for o in all_options)
    total_puts = sum(len(o["puts"]) for o in all_options)
    logger.info(f"{symbol}: Alpaca returning {len(all_options)} expirations, {total_calls} calls, {total_puts} puts")

    return {
        "source": "alpaca",
        "optionChain": {
            "result": [{
                "quote": {"regularMarketPrice": stock_price},
                "expirationDates": exp_timestamps,
                "options": all_options,
            }]
        }
    }



# Curated high-IV watchlist — stocks known for elevated implied volatility
# Covers: crypto-adjacent, meme stocks, biotech, energy/solar, high-growth tech, EV, fintech
HIGH_IV_WATCHLIST = [
    # Crypto / Bitcoin proxy
    "MSTR", "COIN", "RIOT", "MARA", "HUT", "BITF", "CLSK",
    # Meme / retail favorites
    "GME", "AMC", "BBBY", "HOOD", "SOFI", "LCID", "RIVN",
    # High-growth tech / volatile
    "SMCI", "PLTR", "SNOW", "CRWD", "NET", "DKNG", "ROKU", "SNAP", "SQ", "SHOP",
    "ARM", "IONQ", "RGTI", "QUBT",
    # Energy / solar / hydrogen
    "BE", "PLUG", "FSLR", "ENPH", "RUN", "SEDG",
    # Biotech / pharma
    "MRNA", "BNTX", "NVAX",
    # EV
    "TSLA", "NIO", "XPEV", "LI",
    # Large cap tech (high options volume)
    "NVDA", "AMD", "AAPL", "AMZN", "META", "GOOGL", "MSFT", "NFLX",
    # ETFs
    "SPY", "QQQ", "IWM", "ARKK", "XBI",
]


@app.route("/api/active-tickers")
def api_active_tickers():
    """Get most active stock tickers from Alpaca screener, merged with high-IV watchlist."""
    if not ALPACA_API_KEY:
        return jsonify({"error": "Alpaca API not configured"}), 503

    try:
        url = f"{ALPACA_DATA_URL}/v1beta1/screener/stocks/most-actives"
        resp = requests.get(url, headers=_alpaca_headers(), params={"by": "trades", "top": 100}, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        active_tickers = []
        for item in data.get("most_actives", []):
            sym = item.get("symbol", "")
            if not sym or "." in sym or len(sym) > 5:
                continue
            active_tickers.append(sym)

        # Merge: active tickers + high-IV watchlist (deduplicated, preserving order)
        seen = set()
        merged = []
        for sym in active_tickers + HIGH_IV_WATCHLIST:
            if sym not in seen:
                seen.add(sym)
                merged.append(sym)

        # Get snapshot prices to filter out penny stocks (< $15)
        # Process in batches of 50
        filtered = []
        for i in range(0, len(merged), 50):
            batch = merged[i:i+50]
            batch_url = f"{ALPACA_DATA_URL}/v2/stocks/snapshots"
            batch_resp = requests.get(batch_url, headers=_alpaca_headers(),
                                     params={"symbols": ",".join(batch), "feed": "iex"}, timeout=10)
            batch_resp.raise_for_status()
            prices = batch_resp.json()

            for sym in batch:
                snap = prices.get(sym)
                if not snap:
                    continue
                price = None
                if snap.get("latestTrade"):
                    price = snap["latestTrade"].get("p")
                if not price and snap.get("dailyBar"):
                    price = snap["dailyBar"].get("c")
                if price and price >= 15:
                    filtered.append(sym)

        logger.info(f"Active tickers: {len(filtered)} found (active + watchlist)")
        return jsonify({"tickers": filtered})

    except Exception as e:
        logger.error(f"Active tickers failed: {e}")
        return jsonify({"error": str(e)}), 502


@app.route("/api/iv-watchlist")
def api_iv_watchlist():
    """Return the curated high-IV watchlist tickers."""
    return jsonify({"tickers": HIGH_IV_WATCHLIST})


@app.route("/api/iv-scan")
def api_iv_scan():
    """Scan multiple tickers for 30-day ATM implied volatility."""
    tickers_param = request.args.get("tickers", "")
    if not tickers_param:
        return jsonify({"error": "tickers parameter required"}), 400

    tickers = [t.strip().upper() for t in tickers_param.split(",") if t.strip()]
    if len(tickers) > 50:
        tickers = tickers[:50]

    if not ALPACA_API_KEY:
        return jsonify({"error": "Alpaca API not configured"}), 503

    results = []
    now = datetime.now()
    # Target ~30 DTE expiration window
    exp_gte = (now + timedelta(days=20)).strftime("%Y-%m-%d")
    exp_lte = (now + timedelta(days=45)).strftime("%Y-%m-%d")

    for symbol in tickers:
        try:
            price = _alpaca_stock_price(symbol)
            if not price:
                continue

            # Get puts near ATM (within ~10% of stock price)
            strike_lo = price * 0.90
            strike_hi = price * 1.02

            params = {
                "feed": "indicative",
                "type": "put",
                "expiration_date_gte": exp_gte,
                "expiration_date_lte": exp_lte,
                "strike_price_gte": f"{strike_lo:.0f}",
                "strike_price_lte": f"{strike_hi:.0f}",
                "limit": 50,
            }
            url = f"{ALPACA_DATA_URL}/v1beta1/options/snapshots/{symbol}"
            resp = requests.get(url, headers=_alpaca_headers(), params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            snapshots = data.get("snapshots", {})

            if not snapshots:
                continue

            # Find the put closest to ATM with valid IV and ~30 DTE
            best = None
            best_distance = float("inf")
            for occ_sym, snap in snapshots.items():
                _, exp_date, _, strike = _parse_occ_symbol(occ_sym)
                if not strike or not snap.get("impliedVolatility"):
                    continue
                iv = snap["impliedVolatility"]
                if iv <= 0:
                    continue
                greeks = snap.get("greeks", {})
                delta = greeks.get("delta", 0) or 0
                quote = snap.get("latestQuote", {})
                bid = quote.get("bp", 0) or 0
                ask = quote.get("ap", 0) or 0
                # Prefer puts with delta around -0.30 (slightly OTM, good for selling)
                distance = abs(strike - price)
                if distance < best_distance:
                    best_distance = distance
                    best = {
                        "strike": strike,
                        "expiration": exp_date,
                        "iv": round(iv, 4),
                        "delta": round(delta, 4),
                        "bid": bid,
                        "ask": ask,
                        "mid": round((bid + ask) / 2, 2),
                    }

            if best:
                premium_pct = round((best["mid"] / price) * 100, 2)
                # Annualize: (premium / strike) * (365 / dte) * 100
                exp_dt = datetime.strptime(best["expiration"], "%Y-%m-%d")
                dte = (exp_dt - now).days
                ann_yield = round((best["mid"] / best["strike"]) * (365 / max(dte, 1)) * 100, 1) if dte > 0 else 0

                results.append({
                    "symbol": symbol,
                    "price": round(price, 2),
                    "iv30": round(best["iv"] * 100, 1),
                    "atmStrike": best["strike"],
                    "expiration": best["expiration"],
                    "dte": dte,
                    "delta": best["delta"],
                    "bid": best["bid"],
                    "ask": best["ask"],
                    "mid": best["mid"],
                    "premiumPct": premium_pct,
                    "annualizedYield": ann_yield,
                })
                logger.info(f"IV scan {symbol}: price=${price:.2f}, IV={best['iv']*100:.1f}%, mid=${best['mid']}")

        except Exception as e:
            logger.warning(f"IV scan failed for {symbol}: {e}")

    # Sort by IV descending
    results.sort(key=lambda x: x["iv30"], reverse=True)

    return jsonify({"results": results})


@app.route("/api/yahoo/options/<symbol>")
def api_yahoo_options(symbol):
    """Fetch stock price + options data. Uses Alpaca (primary) or Yahoo (fallback)."""
    symbol = symbol.upper()

    # Try Alpaca first if credentials are available
    if ALPACA_API_KEY and ALPACA_SECRET_KEY:
        try:
            return _fetch_alpaca(symbol)
        except Exception as e:
            logger.warning(f"Alpaca failed for {symbol}, falling back to Yahoo: {e}")

    # Fallback to Yahoo
    return _fetch_yahoo(symbol)


def _fetch_alpaca(symbol):
    """Fetch from Alpaca API."""
    stock_price = _alpaca_stock_price(symbol)
    if not stock_price:
        raise ValueError(f"No Alpaca price for {symbol}")

    logger.info(f"{symbol} price (Alpaca): ${stock_price:.2f}")

    now = datetime.now()
    exp_gte = (now + timedelta(days=25)).strftime("%Y-%m-%d")
    exp_lte = (now + timedelta(days=90)).strftime("%Y-%m-%d")

    call_snapshots = _alpaca_options_chain(symbol, "call", exp_gte, exp_lte)
    put_snapshots = _alpaca_options_chain(symbol, "put", exp_gte, exp_lte)

    logger.info(f"{symbol}: Alpaca returned {len(call_snapshots)} call snapshots, {len(put_snapshots)} put snapshots")

    return jsonify(_build_alpaca_response(symbol, stock_price, call_snapshots, put_snapshots))


def _fetch_yahoo(symbol):
    """Fallback: Fetch from Yahoo Finance via yfinance."""
    try:
        ticker = yf.Ticker(symbol)
        stock_price = _get_yf_stock_price(ticker, symbol)
        if not stock_price:
            return jsonify({"error": f"No price data for {symbol}"}), 404

        logger.info(f"{symbol} price (Yahoo): ${stock_price:.2f}")

        exp_dates, valid_exps = _get_valid_expirations(ticker, symbol)
        if not valid_exps:
            return _empty_response(stock_price, "yfinance")

        all_options = []
        for exp_str in valid_exps[:8]:
            try:
                chain = ticker.option_chain(exp_str)
                exp_dt = datetime.strptime(exp_str, "%Y-%m-%d")
                exp_ts = int(exp_dt.timestamp())

                calls_list = _build_yf_option_list(chain.calls, exp_ts)
                puts_list = _build_yf_option_list(chain.puts, exp_ts)

                all_options.append({
                    "expirationDate": exp_ts,
                    "calls": calls_list,
                    "puts": puts_list,
                })
            except Exception as e:
                logger.warning(f"Failed fetching {symbol} options for {exp_str}: {e}")

        exp_timestamps = [int(datetime.strptime(e, "%Y-%m-%d").timestamp()) for e in valid_exps]

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
        logger.error(f"Yahoo failed for {symbol}: {e}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 502


# ── Yahoo/yfinance helpers ──

def _get_yf_stock_price(ticker, symbol):
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

    return exp_dates, valid_exps


def _empty_response(stock_price, source="yfinance"):
    return jsonify({
        "source": source,
        "optionChain": {
            "result": [{
                "quote": {"regularMarketPrice": stock_price or 0},
                "expirationDates": [],
                "options": [],
            }]
        }
    })


def _build_yf_option_list(df, exp_ts):
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


def _is_nan(val):
    try:
        return val != val
    except Exception:
        return False


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(debug=debug, host="0.0.0.0", port=port)
