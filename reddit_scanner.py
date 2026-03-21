"""
reddit_scanner.py — r/wallstreetbets ticker scanner
Scrapes hot/top posts from r/wallstreetbets (public JSON, no auth needed).
Extracts stock tickers, scores by engagement + recency, returns ranked list.
Integrates as a Flask Blueprint into the MOMO INDEX pipeline.
"""

from flask import Blueprint, jsonify
import requests
import re
import time
from datetime import datetime, timezone
from collections import defaultdict

reddit_bp = Blueprint('reddit', __name__)

# ── Ticker extraction config ────────────────────────────────────────────────

# Noise words that look like tickers but aren't
STOPWORDS = {
    'A', 'I', 'AM', 'AN', 'AS', 'AT', 'BE', 'BY', 'DO', 'GO', 'IF', 'IN',
    'IS', 'IT', 'ME', 'MY', 'NO', 'OF', 'OK', 'ON', 'OR', 'SO', 'TO', 'UP',
    'US', 'WE', 'AI', 'TV', 'EV', 'PR', 'VC', 'PM', 'AM', 'CEO', 'CFO',
    'CTO', 'CMO', 'COO', 'IPO', 'ETF', 'ATM', 'OTM', 'ITM', 'IMO', 'IMO',
    'WSB', 'DD', 'IV', 'OI', 'PE', 'PB', 'EPS', 'FCF', 'NAV', 'AUM',
    'API', 'URL', 'GDP', 'CPI', 'FED', 'SEC', 'NYSE', 'CBOE', 'OTC',
    'SPY', 'QQQ', 'IWM', 'VIX',  # keep ETFs but flag separately
    'YOLO', 'FOMO', 'MOAT', 'MOON', 'PUTS', 'CALL', 'HOLD', 'SELL', 'BUY',
    'LMAO', 'LMFAO', 'WTF', 'LOL', 'TBH', 'IMO', 'FYI', 'TLDR', 'AMA',
    'AND', 'THE', 'FOR', 'NOT', 'BUT', 'ARE', 'WAS', 'HAS', 'HAD', 'HIM',
    'HER', 'HIS', 'ITS', 'OUR', 'OUT', 'NEW', 'NOW', 'HOW', 'WHY', 'WHO',
    'ALL', 'ANY', 'CAN', 'DID', 'GET', 'GOT', 'LET', 'MAY', 'PUT', 'RUN',
    'SAW', 'SET', 'SAY', 'SEE', 'TOO', 'TWO', 'USE', 'WAY', 'YET', 'JUST',
    'BEEN', 'COME', 'DOES', 'DONE', 'EACH', 'EVEN', 'FIND', 'FROM', 'GOOD',
    'HAVE', 'HERE', 'KNOW', 'LAST', 'LIKE', 'LONG', 'LOOK', 'MAKE', 'MORE',
    'MOST', 'MUCH', 'NEED', 'NEXT', 'ONLY', 'OVER', 'SAME', 'SOME', 'TAKE',
    'THAN', 'THAT', 'THEM', 'THEN', 'THEY', 'THIS', 'TIME', 'WEEK', 'WELL',
    'WENT', 'WERE', 'WHAT', 'WHEN', 'WITH', 'WILL', 'YOUR', 'ALSO', 'BACK',
    'CALL', 'CASH', 'COST', 'GAIN', 'HIGH', 'HUGE', 'INTO', 'KEEP', 'KILL',
    'LOSS', 'LOSE', 'MOVE', 'OPEN', 'PLAY', 'POOR', 'PUMP', 'RICH', 'RISK',
    'SAID', 'SELL', 'SHIP', 'SHOW', 'SLOW', 'STOP', 'SURE', 'VERY', 'WANT',
    'WORK', 'YEAR', 'ZERO', 'DOWN', 'DROP', 'DUMP', 'BULL', 'BEAR', 'MOON',
    'APES', 'KING', 'EPIC', 'RATE', 'DEBT', 'FUND', 'REAL', 'PLAN', 'DEAL',
    'BULL', 'BEAR', 'MEME', 'BANK', 'FIRM', 'FEAR', 'HOPE', 'NEWS', 'DATA',
    'LINK', 'POST', 'EDIT', 'NOTE', 'FULL', 'FREE', 'SAFE', 'HARD', 'EASY',
    'HUGE', 'FAST', 'HELD', 'SOLD', 'TOLD', 'WAIT', 'MAIN', 'BASE', 'BEST',
    'CASE', 'FLOW', 'LAND', 'LESS', 'LIFE', 'LINE', 'LIST', 'LIVE', 'LOAD',
    'LOCK', 'PAST', 'PATH', 'PICK', 'PUSH', 'READ', 'REST', 'SEND', 'SIDE',
    'SIZE', 'SORT', 'STAY', 'STEP', 'TOPS', 'TURN', 'TYPE', 'VIEW', 'VOTE',
    'WIDE', 'WINS', 'WITH', 'WORD',
    'THERE', 'THEIR', 'ABOUT', 'AFTER', 'AGAIN', 'BELOW', 'COULD', 'EVERY',
    'FIRST', 'FLOOR', 'GREAT', 'LOOKS', 'MAYBE', 'MIGHT', 'OTHER', 'PLACE',
    'PRICE', 'PRINT', 'RALLY', 'SHARE', 'SHORT', 'SINCE', 'STOCK', 'STILL',
    'SWING', 'THINK', 'THREE', 'TRADE', 'UNDER', 'UNTIL', 'USING', 'VALUE',
    'WATCH', 'WHERE', 'WHICH', 'WHILE', 'WHOLE', 'WHOSE', 'WOULD', 'WORTH',
    'WRITE', 'WRONG', 'ABOVE', 'AHEAD', 'CALLS', 'CHART', 'CHEAP', 'COVER',
    'DOING', 'EARLY', 'GAINS', 'GOING', 'HOLDS', 'HOURS', 'LARGE', 'LATER',
    'LEVEL', 'LOWER', 'MACRO', 'MAKES', 'MONEY', 'NEVER', 'NOPE', 'OFTEN',
    'POINT', 'POWER', 'PUMPS', 'READY', 'RIGHT', 'RISKS', 'SMALL', 'SPLIT',
    'SQUEEZE', 'STRONG', 'TARGET', 'THESIS', 'TODAY', 'TRIED', 'TRULY',
    'ULTRA', 'UPSIDE', 'USUAL', 'WEEKS', 'YEARS',
}

# Regex: cashtag $TICK or standalone 2-5 uppercase letter word
CASHTAG_RE = re.compile(r'\$([A-Z]{1,5})\b')
WORD_RE = re.compile(r'\b([A-Z]{2,5})\b')

# Known valid tickers we always accept (to reduce false negatives)
KNOWN_TICKERS = {
    'NVDA', 'TSLA', 'AAPL', 'META', 'AMZN', 'MSFT', 'GOOGL', 'GOOG',
    'AMD', 'COIN', 'MSTR', 'HOOD', 'PLTR', 'SOFI', 'SMCI', 'RIVN',
    'SPOT', 'CRWD', 'MELI', 'SHOP', 'SPY', 'QQQ', 'IWM', 'GME', 'AMC',
    'BBBY', 'LCID', 'SNOW', 'NET', 'DKNG', 'ROKU', 'SNAP', 'SQ', 'SHOP',
    'ARM', 'IONQ', 'RGTI', 'QUBT', 'BE', 'PLUG', 'FSLR', 'ENPH', 'RUN',
    'MRNA', 'BNTX', 'NIO', 'XPEV', 'NFLX', 'RIOT', 'MARA', 'HUT',
    'BITF', 'CLSK', 'UBER', 'LYFT', 'ABNB', 'DASH', 'RBLX', 'TWLO',
    'ZM', 'DOCU', 'BILL', 'AFRM', 'UPST', 'HOOD', 'NU', 'HIMS', 'RDDT',
    'APP', 'META', 'PINS', 'SNAP', 'MTCH', 'BMBL', 'DUOL', 'CELH',
    'DIS', 'NFLX', 'WBD', 'PARA', 'FUBO', 'SIRI', 'T', 'VZ', 'TMUS',
    'INTC', 'QCOM', 'TXN', 'AVGO', 'MU', 'WDC', 'STX', 'LRCX', 'AMAT',
    'KLAC', 'ASML', 'TSM', 'ORCL', 'SAP', 'CRM', 'NOW', 'WDAY', 'ADBE',
    'INTU', 'PANW', 'ZS', 'S', 'FTNT', 'OKTA', 'CYLR', 'DDOG', 'GTLB',
    'MDB', 'ESTC', 'CFLT', 'RBRK', 'PATH', 'AI', 'BBAI', 'SOUN', 'IREN',
    'CORZ', 'BTBT', 'BTDR', 'CIFR', 'WULF', 'BRPH',
}


def extract_tickers(text):
    """Extract ticker symbols from a block of text.
    Cashtags ($NVDA) get higher confidence than bare words."""
    tickers = defaultdict(lambda: {'count': 0, 'cashtag': False})

    # Cashtags are high-confidence
    for m in CASHTAG_RE.finditer(text.upper()):
        sym = m.group(1)
        tickers[sym]['count'] += 2
        tickers[sym]['cashtag'] = True

    # Bare uppercase words — filter aggressively
    for m in WORD_RE.finditer(text):
        sym = m.group(1).upper()
        if sym in STOPWORDS:
            continue
        if sym in KNOWN_TICKERS or (len(sym) >= 2 and sym not in STOPWORDS):
            tickers[sym]['count'] += 1

    return tickers


def score_post(upvotes, num_comments, created_utc, now_ts):
    """Engagement score: upvotes + comment weight + recency decay."""
    age_hours = max((now_ts - created_utc) / 3600, 0.1)
    recency = 1.0 / (1.0 + age_hours / 12)  # half-life ~12 hours
    raw = (upvotes * 1.0 + num_comments * 3.0)
    return raw * recency


def fetch_wsb_posts(limit=100, sort='hot'):
    """Fetch posts from r/wallstreetbets via public JSON API."""
    url = f"https://www.reddit.com/r/wallstreetbets/{sort}.json"
    headers = {'User-Agent': 'MomoIndex/1.0 (ticker scanner; contact@mrpage.io)'}
    try:
        resp = requests.get(url, headers=headers, params={'limit': limit}, timeout=10)
        if resp.status_code != 200:
            return []
        data = resp.json()
        return data.get('data', {}).get('children', [])
    except Exception as e:
        print(f"Reddit fetch error ({sort}): {e}")
        return []


def fetch_wsb_comments(limit=500):
    """Fetch recent comments from r/wallstreetbets for ticker frequency."""
    url = "https://www.reddit.com/r/wallstreetbets/comments.json"
    headers = {'User-Agent': 'MomoIndex/1.0 (ticker scanner; contact@mrpage.io)'}
    try:
        resp = requests.get(url, headers=headers, params={'limit': limit}, timeout=10)
        if resp.status_code != 200:
            return []
        data = resp.json()
        return data.get('data', {}).get('children', [])
    except Exception as e:
        print(f"Reddit comments fetch error: {e}")
        return []


def get_momo_signals(universe: list) -> dict:
    """
    Returns {sym: score_0_100} for tickers in universe based on WSB mention data.
    Called by momo_api.py background scheduler to blend into MOMO scores.
    """
    now_ts = time.time()
    universe_set = set(universe)
    ticker_agg = defaultdict(lambda: {'mentions': 0, 'eng_score': 0.0})

    hot_posts  = fetch_wsb_posts(limit=100, sort='hot')
    new_posts  = fetch_wsb_posts(limit=50,  sort='new')
    all_posts  = hot_posts + new_posts

    for child in all_posts:
        post = child.get('data', {})
        flair = (post.get('link_flair_text') or '').lower()
        if flair in ('weekend discussion', 'daily discussion', 'meme', 'shitpost'):
            continue
        title    = post.get('title', '')
        selftext = post.get('selftext', '') or ''
        combined = title + ' ' + title + ' ' + selftext[:500]
        upvotes  = post.get('score', 0) or 0
        comments = post.get('num_comments', 0) or 0
        created  = post.get('created_utc', now_ts)
        eng      = score_post(upvotes, comments, created, now_ts)
        tickers  = extract_tickers(combined)
        for sym, info in tickers.items():
            if sym in universe_set:
                ticker_agg[sym]['mentions']  += info['count']
                ticker_agg[sym]['eng_score'] += eng * info['count']

    recent_comments = fetch_wsb_comments(limit=200)
    for child in recent_comments:
        comment = child.get('data', {})
        body    = comment.get('body', '') or ''
        score   = comment.get('score', 1) or 1
        created = comment.get('created_utc', now_ts)
        eng     = score_post(max(score, 0), 0, created, now_ts) * 0.3
        tickers = extract_tickers(body[:300])
        for sym, info in tickers.items():
            if sym in universe_set:
                ticker_agg[sym]['mentions']  += info['count']
                ticker_agg[sym]['eng_score'] += eng

    if not ticker_agg:
        return {}

    max_eng      = max(d['eng_score'] for d in ticker_agg.values()) or 1
    max_mentions = max(d['mentions']  for d in ticker_agg.values()) or 1
    result = {}
    for sym, data in ticker_agg.items():
        norm_eng      = data['eng_score'] / max_eng      * 60
        norm_mentions = data['mentions']  / max_mentions * 40
        result[sym]   = round(min(norm_eng + norm_mentions, 100), 1)
    return result


@reddit_bp.route('/api/reddit/wsb')
def wsb_scanner():
    """
    GET /api/reddit/wsb
    Returns top tickers mentioned in r/wallstreetbets with engagement scores.
    """
    now_ts = time.time()

    # Aggregate ticker mentions across hot + rising posts + recent comments
    ticker_agg = defaultdict(lambda: {
        'mentions': 0,
        'eng_score': 0.0,
        'cashtag_mentions': 0,
        'post_titles': [],
        'upvotes': 0,
    })

    # Fetch hot posts
    hot_posts = fetch_wsb_posts(limit=100, sort='hot')
    # Fetch new posts for recency signal
    new_posts = fetch_wsb_posts(limit=50, sort='new')
    all_posts = hot_posts + new_posts

    for child in all_posts:
        post = child.get('data', {})
        title = post.get('title', '')
        selftext = post.get('selftext', '') or ''
        upvotes = post.get('score', 0) or 0
        num_comments = post.get('num_comments', 0) or 0
        created_utc = post.get('created_utc', now_ts)
        flair = post.get('link_flair_text', '') or ''

        # Skip mod/meta posts
        if flair.lower() in ('weekend discussion', 'daily discussion', 'meme', 'shitpost'):
            continue

        # Combined text (title weighted more)
        combined = title + ' ' + title + ' ' + selftext[:500]
        tickers = extract_tickers(combined)

        eng = score_post(upvotes, num_comments, created_utc, now_ts)

        for sym, info in tickers.items():
            if len(sym) < 1 or len(sym) > 5:
                continue
            ticker_agg[sym]['mentions'] += info['count']
            ticker_agg[sym]['eng_score'] += eng * info['count']
            ticker_agg[sym]['upvotes'] += upvotes
            if info['cashtag']:
                ticker_agg[sym]['cashtag_mentions'] += 1
            if title and len(ticker_agg[sym]['post_titles']) < 2:
                ticker_agg[sym]['post_titles'].append(title[:100])

    # Fetch recent comments for mention frequency
    recent_comments = fetch_wsb_comments(limit=200)
    for child in recent_comments:
        comment = child.get('data', {})
        body = comment.get('body', '') or ''
        score = comment.get('score', 1) or 1
        created_utc = comment.get('created_utc', now_ts)

        tickers = extract_tickers(body[:300])
        eng = score_post(max(score, 0), 0, created_utc, now_ts) * 0.3  # comments weighted less

        for sym, info in tickers.items():
            if len(sym) < 1 or len(sym) > 5:
                continue
            ticker_agg[sym]['mentions'] += info['count']
            ticker_agg[sym]['eng_score'] += eng

    # Filter: must have 2+ mentions OR be a known ticker with 1+ mention
    filtered = {
        sym: data for sym, data in ticker_agg.items()
        if (data['mentions'] >= 2 or (sym in KNOWN_TICKERS and data['mentions'] >= 1))
        and sym not in STOPWORDS
    }

    # Compute final momo score: blend mentions + engagement
    results = []
    max_eng = max((d['eng_score'] for d in filtered.values()), default=1) or 1
    max_mentions = max((d['mentions'] for d in filtered.values()), default=1) or 1

    for sym, data in filtered.items():
        norm_eng = data['eng_score'] / max_eng * 60
        norm_mentions = data['mentions'] / max_mentions * 40
        momo_score = round(norm_eng + norm_mentions)

        results.append({
            'sym': sym,
            'mentions': data['mentions'],
            'cashtag_mentions': data['cashtag_mentions'],
            'eng_score': round(data['eng_score'], 1),
            'upvotes': data['upvotes'],
            'momoScore': momo_score,
            'sample_titles': data['post_titles'],
            'known': sym in KNOWN_TICKERS,
        })

    results.sort(key=lambda x: x['momoScore'], reverse=True)

    return jsonify({
        'source': 'reddit_wsb',
        'tickers': results[:50],
        'total_posts_scanned': len(all_posts),
        'total_comments_scanned': len(recent_comments),
        'fetchedAt': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    })


@reddit_bp.route('/api/reddit/wsb/ticker/<sym>')
def wsb_ticker(sym):
    """
    GET /api/reddit/wsb/ticker/NVDA
    Returns WSB mention stats for a single ticker.
    """
    sym = sym.upper()
    now_ts = time.time()
    results = []

    hot_posts = fetch_wsb_posts(limit=100, sort='hot')
    for child in hot_posts:
        post = child.get('data', {})
        title = post.get('title', '')
        selftext = post.get('selftext', '') or ''
        combined = title + ' ' + selftext[:500]

        if sym in combined.upper():
            tickers = extract_tickers(combined)
            if sym in tickers:
                results.append({
                    'title': title[:120],
                    'score': post.get('score', 0),
                    'comments': post.get('num_comments', 0),
                    'url': f"https://reddit.com{post.get('permalink', '')}",
                    'created_utc': post.get('created_utc'),
                })

    return jsonify({
        'sym': sym,
        'posts': results[:10],
        'mention_count': len(results),
        'fetchedAt': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    })
