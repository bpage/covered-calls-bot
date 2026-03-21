"""
momo_api.py — MOMO Index backend
Blends StockTwits, Reddit (r/wsb), and X cashtag signals into a unified MOMO score.
Background thread refreshes social data every 20 minutes.

Score weights:
  StockTwits : 60%  (volume surge + bull/bear sentiment, fetched live)
  Reddit     : 25%  (r/wsb engagement — no credentials needed)
  X          : 15%  (cashtag engagement — requires X_BEARER_TOKEN env var)

If X_BEARER_TOKEN is absent, ST/Reddit weights auto-redistribute to 70/30.
"""

import time
import threading
import logging
from flask import Blueprint, jsonify
import requests

# Social signal helpers from sibling blueprints
try:
    from reddit_scanner import get_momo_signals as _reddit_signals
except ImportError:
    def _reddit_signals(universe): return {}

try:
    from x_scanner import get_momo_signals as _x_signals
except ImportError:
    def _x_signals(universe): return {}

log = logging.getLogger(__name__)

momo_bp = Blueprint('momo', __name__)

UNIVERSE = [
    'NVDA', 'TSLA', 'AAPL', 'META', 'AMZN', 'MSFT', 'GOOGL', 'AMD',
    'COIN', 'MSTR', 'HOOD', 'PLTR', 'SOFI', 'SMCI', 'RIVN',
    'SPOT', 'CRWD', 'MELI', 'SHOP', 'SPY',
]

NAMES = {
    'NVDA': 'Nvidia', 'TSLA': 'Tesla', 'AAPL': 'Apple', 'META': 'Meta',
    'AMZN': 'Amazon', 'MSFT': 'Microsoft', 'GOOGL': 'Alphabet', 'AMD': 'AMD',
    'COIN': 'Coinbase', 'MSTR': 'MicroStrategy', 'HOOD': 'Robinhood',
    'PLTR': 'Palantir', 'SOFI': 'SoFi', 'SMCI': 'Supermicro', 'RIVN': 'Rivian',
    'SPOT': 'Spotify', 'CRWD': 'CrowdStrike', 'MELI': 'MercadoLibre',
    'SHOP': 'Shopify', 'SPY': 'S&P 500 ETF',
}

# ─── Score weights ───────────────────────────────────────────────────────────
_W_ST     = 0.60  # StockTwits
_W_REDDIT = 0.25  # Reddit WSB
_W_X      = 0.15  # X / Twitter

SCAN_INTERVAL_MINUTES = 20

# ─── In-memory social cache ──────────────────────────────────────────────────
_lock  = threading.Lock()
_cache = {
    'reddit':        {},    # {sym: 0-100}
    'x':             {},    # {sym: 0-100}
    'last_scan_at':  None,
    'scan_count':    0,
}


def _run_social_scan():
    t0 = time.time()
    log.info('[momo] Social scan starting...')
    reddit_scores = {}
    x_scores      = {}
    try:
        reddit_scores = _reddit_signals(UNIVERSE)
    except Exception as e:
        log.error(f'[momo] Reddit scan error: {e}')
    try:
        x_scores = _x_signals(UNIVERSE)
    except Exception as e:
        log.error(f'[momo] X scan error: {e}')

    elapsed = round(time.time() - t0, 1)
    log.info(
        f'[momo] Social scan done in {elapsed}s — '
        f'Reddit: {len(reddit_scores)} tickers, X: {len(x_scores)} tickers'
    )
    if reddit_scores:
        top_r = sorted(reddit_scores.items(), key=lambda kv: -kv[1])[:5]
        log.info(f'[momo] Reddit top5: {top_r}')
    if x_scores:
        top_x = sorted(x_scores.items(), key=lambda kv: -kv[1])[:5]
        log.info(f'[momo] X top5: {top_x}')

    with _lock:
        _cache['reddit']       = reddit_scores
        _cache['x']            = x_scores
        _cache['last_scan_at'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        _cache['scan_count']  += 1


def _scheduler_loop():
    time.sleep(10)  # let Flask finish booting
    while True:
        try:
            _run_social_scan()
        except Exception as e:
            log.error(f'[momo] Scheduler error: {e}')
        time.sleep(SCAN_INTERVAL_MINUTES * 60)


# Start scheduler daemon on import
_t = threading.Thread(target=_scheduler_loop, name='social-scheduler', daemon=True)
_t.start()
log.info(f'[momo] Social scanner started — interval={SCAN_INTERVAL_MINUTES}m')


# ─── StockTwits fetcher ──────────────────────────────────────────────────────

def fetch_stocktwits(sym):
    url = f'https://api.stocktwits.com/api/2/streams/symbol/{sym}.json?limit=30'
    try:
        res = requests.get(url, timeout=8, headers={'User-Agent': 'Mozilla/5.0'})
        if res.status_code != 200:
            return None
        data     = res.json()
        messages = data.get('messages', [])

        bull_count = bear_count = 0
        posts = []
        for m in messages:
            sent = (m.get('entities') or {}).get('sentiment', {})
            sentiment = sent.get('basic') if sent else None
            if sentiment == 'Bullish':
                bull_count += 1
            elif sentiment == 'Bearish':
                bear_count += 1
            posts.append({
                'body':      m.get('body', '')[:140],
                'sentiment': sentiment or 'Neutral',
                'user':      m.get('user', {}).get('username', 'trader'),
                'followers': m.get('user', {}).get('followers', 0),
                'time':      m.get('created_at', ''),
            })

        total    = len(messages) or 1
        bull_pct = round(bull_count / total * 100)
        bear_pct = 100 - bull_pct

        # StockTwits component: volume surge + sentiment strength (0-100)
        vol_score = min(len(messages) / 30 * 40, 40)
        st_score  = round(vol_score + bull_pct * 0.6)

        return {
            'sym':       sym,
            'name':      NAMES.get(sym, sym),
            'bullCount': bull_count,
            'bearCount': bear_count,
            'bullPct':   bull_pct,
            'bearPct':   bear_pct,
            'total':     len(messages),
            'stScore':   st_score,
            'posts':     posts[:3],
        }
    except Exception as e:
        log.warning(f'[stocktwits] Error fetching {sym}: {e}')
        return None


def _blend(sym: str, st_score: float) -> dict:
    """Blend StockTwits + Reddit + X into a single momoScore."""
    with _lock:
        r_score  = _cache['reddit'].get(sym, 0.0)
        x_score  = _cache['x'].get(sym, 0.0)
        has_x    = bool(_cache['x'])

    if has_x:
        w_st, w_r, w_x = _W_ST, _W_REDDIT, _W_X
    else:
        # Redistribute X weight proportionally between ST and Reddit
        total = _W_ST + _W_REDDIT
        w_st, w_r, w_x = _W_ST / total, _W_REDDIT / total, 0.0

    momo_score = round(st_score * w_st + r_score * w_r + x_score * w_x)
    return {
        'momoScore':   momo_score,
        'stScore':     round(st_score, 1),
        'redditScore': round(r_score, 1),
        'xScore':      round(x_score, 1) if has_x else None,
    }


# ─── Routes ──────────────────────────────────────────────────────────────────

@momo_bp.route('/api/momo')
def momo_index():
    """
    GET /api/momo
    Returns blended social + StockTwits MOMO scores for all tickers.
    """
    with _lock:
        last_scan_at = _cache['last_scan_at']
        scan_count   = _cache['scan_count']
        sources = {
            'stocktwits': True,
            'reddit':     bool(_cache['reddit']),
            'x':          bool(_cache['x']),
        }

    results = []
    for sym in UNIVERSE:
        data = fetch_stocktwits(sym)
        if not data:
            continue
        blend = _blend(sym, data['stScore'])
        results.append({**data, **blend})
        time.sleep(0.15)

    if not results:
        return jsonify({'error': 'StockTwits unavailable'}), 503

    log.info(f'[momo] /api/momo served {len(results)} tickers (scan #{scan_count})')
    return jsonify({
        'stocks':           results,
        'fetchedAt':        time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'lastSocialScanAt': last_scan_at,
        'socialScanCount':  scan_count,
        'sources':          sources,
    })


@momo_bp.route('/api/momo/ticker/<sym>')
def momo_ticker(sym):
    """
    GET /api/momo/ticker/NVDA
    Single ticker with full score breakdown.
    """
    sym = sym.upper()
    if sym not in UNIVERSE:
        return jsonify({'error': 'Ticker not in universe'}), 404
    data = fetch_stocktwits(sym)
    if not data:
        return jsonify({'error': 'Failed to fetch StockTwits data'}), 503
    blend = _blend(sym, data['stScore'])
    return jsonify({**data, **blend})


@momo_bp.route('/api/momo/social-status')
def social_status():
    """
    GET /api/momo/social-status
    Returns scan health — last scan time, ticker counts, top 5 per source.
    """
    with _lock:
        reddit_top = sorted(_cache['reddit'].items(), key=lambda kv: -kv[1])[:5]
        x_top      = sorted(_cache['x'].items(),      key=lambda kv: -kv[1])[:5]
        return jsonify({
            'lastScanAt':    _cache['last_scan_at'],
            'scanCount':     _cache['scan_count'],
            'scanIntervalM': SCAN_INTERVAL_MINUTES,
            'reddit': {
                'tickersFound': len(_cache['reddit']),
                'top5': reddit_top,
            },
            'x': {
                'enabled':      bool(_cache['x']),
                'tickersFound': len(_cache['x']),
                'top5': x_top,
            },
        })
