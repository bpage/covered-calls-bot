"""
x_scanner.py — X (Twitter) cashtag scanner
Searches X for $TICKER cashtag mentions using X API v2.
Scores by engagement (likes, retweets, replies) + recency.
Requires env var: X_BEARER_TOKEN

Integrates as a Flask Blueprint into the MOMO INDEX pipeline.
"""

from flask import Blueprint, jsonify
import requests
import os
import time
import re
from datetime import datetime, timezone, timedelta
from collections import defaultdict

x_bp = Blueprint('x_scanner', __name__)

X_BEARER_TOKEN = os.environ.get('X_BEARER_TOKEN', '')
X_API_BASE = 'https://api.twitter.com/2'

# Same stopwords as reddit_scanner — tickers we know aren't stocks
STOPWORDS = {
    'A', 'I', 'AM', 'AN', 'AS', 'AT', 'BE', 'BY', 'DO', 'GO', 'IF', 'IN',
    'IS', 'IT', 'ME', 'MY', 'NO', 'OF', 'OK', 'ON', 'OR', 'SO', 'TO', 'UP',
    'US', 'WE', 'AI', 'TV', 'EV', 'PR', 'VC', 'PM', 'CEO', 'CFO', 'CTO',
    'CMO', 'COO', 'IPO', 'ETF', 'ATM', 'OTM', 'ITM', 'IMO', 'WSB', 'DD',
    'IV', 'OI', 'PE', 'PB', 'EPS', 'FCF', 'NAV', 'AUM', 'API', 'GDP',
    'CPI', 'FED', 'SEC', 'NYSE', 'CBOE', 'OTC', 'VIX',
    'YOLO', 'FOMO', 'LMAO', 'WTF', 'LOL', 'TBH', 'FYI', 'TLDR', 'AMA',
    'AND', 'THE', 'FOR', 'NOT', 'BUT', 'ARE', 'WAS', 'HAS', 'HAD',
    'NEW', 'NOW', 'HOW', 'WHY', 'WHO', 'ALL', 'ANY', 'CAN', 'DID', 'GET',
    'BUY', 'SELL', 'HOLD', 'CALL', 'PUTS', 'BULL', 'BEAR', 'MOON', 'DUMP',
    'PUMP', 'LONG', 'SHORT', 'CASH', 'RISK', 'RATE', 'GAIN', 'LOSS',
    'MEME', 'APES', 'YEET',
}


def _x_headers():
    return {'Authorization': f'Bearer {X_BEARER_TOKEN}'}


def _score_tweet(metrics, created_at_str, now_dt):
    """Score a tweet by engagement + recency."""
    likes = metrics.get('like_count', 0) or 0
    retweets = metrics.get('retweet_count', 0) or 0
    replies = metrics.get('reply_count', 0) or 0
    quotes = metrics.get('quote_count', 0) or 0

    try:
        created = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
        age_hours = max((now_dt - created).total_seconds() / 3600, 0.1)
    except Exception:
        age_hours = 12

    # Recency decay: half-life ~6 hours (X moves fast)
    recency = 1.0 / (1.0 + age_hours / 6)
    raw = likes * 1.0 + retweets * 2.0 + replies * 1.5 + quotes * 1.5
    return raw * recency


def fetch_cashtag_tweets(max_results=100):
    """
    Fetch recent tweets that contain any cashtag using X API v2 recent search.
    Uses `has:cashtags` operator to pull financial tweets efficiently.
    Returns list of tweet objects with entities and public_metrics.
    """
    # Search: English tweets with cashtags, no retweets, last 24h
    since = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%SZ')
    query = 'has:cashtags lang:en -is:retweet -is:reply'

    params = {
        'query': query,
        'max_results': min(max_results, 100),
        'tweet.fields': 'created_at,entities,public_metrics,text',
        'start_time': since,
        'sort_order': 'recency',
    }

    try:
        resp = requests.get(
            f'{X_API_BASE}/tweets/search/recent',
            headers=_x_headers(),
            params=params,
            timeout=15,
        )
        if resp.status_code == 401:
            return None, 'unauthorized'
        if resp.status_code == 403:
            return None, 'forbidden'
        if resp.status_code != 200:
            return None, f'http_{resp.status_code}'
        data = resp.json()
        return data.get('data', []), None
    except Exception as e:
        return None, str(e)


def extract_cashtags_from_tweet(tweet):
    """
    Extract cashtags from tweet entities (preferred) or regex fallback.
    Returns list of uppercase ticker symbols.
    """
    # Preferred: use entities.cashtags from API response
    entities = tweet.get('entities') or {}
    cashtags = entities.get('cashtags') or []
    if cashtags:
        return [ct['tag'].upper() for ct in cashtags if ct.get('tag')]

    # Fallback: regex on tweet text
    text = tweet.get('text', '')
    return re.findall(r'\$([A-Z]{1,5})\b', text.upper())


def get_momo_signals(universe: list) -> dict:
    """
    Returns {sym: score_0_100} for tickers in universe based on X cashtag data.
    Called by momo_api.py background scheduler to blend into MOMO scores.
    Returns empty dict if X_BEARER_TOKEN is not set.
    """
    if not X_BEARER_TOKEN:
        return {}

    universe_set = set(universe)
    now_dt = datetime.now(timezone.utc)
    tweets, err = fetch_cashtag_tweets(max_results=100)
    if err or not tweets:
        return {}

    ticker_eng = defaultdict(float)
    for tweet in tweets:
        tickers  = extract_cashtags_from_tweet(tweet)
        metrics  = tweet.get('public_metrics') or {}
        created_at = tweet.get('created_at', '')
        eng      = _score_tweet(metrics, created_at, now_dt)
        for sym in tickers:
            if sym in universe_set:
                ticker_eng[sym] += eng

    if not ticker_eng:
        return {}

    max_eng = max(ticker_eng.values()) or 1
    return {
        sym: round(min(score / max_eng * 100, 100), 1)
        for sym, score in ticker_eng.items()
        if sym in universe_set
    }


@x_bp.route('/api/x/cashtags')
def x_cashtag_scanner():
    """
    GET /api/x/cashtags
    Returns top tickers from X cashtag mentions with engagement scores.
    Requires X_BEARER_TOKEN env var.
    """
    if not X_BEARER_TOKEN:
        return jsonify({
            'error': 'X_BEARER_TOKEN not configured',
            'setup': 'Set X_BEARER_TOKEN env var in Render dashboard with an X API v2 Bearer token.',
        }), 503

    now_dt = datetime.now(timezone.utc)
    tweets, err = fetch_cashtag_tweets(max_results=100)

    if err:
        return jsonify({'error': f'X API error: {err}'}), 502
    if not tweets:
        return jsonify({'tickers': [], 'total_tweets_scanned': 0,
                        'fetchedAt': now_dt.strftime('%Y-%m-%dT%H:%M:%SZ')})

    # Aggregate by ticker
    ticker_agg = defaultdict(lambda: {
        'mentions': 0,
        'eng_score': 0.0,
        'likes': 0,
        'retweets': 0,
        'sample_texts': [],
    })

    for tweet in tweets:
        tickers = extract_cashtags_from_tweet(tweet)
        metrics = tweet.get('public_metrics') or {}
        created_at = tweet.get('created_at', '')
        text = tweet.get('text', '')[:140]

        eng = _score_tweet(metrics, created_at, now_dt)

        for sym in tickers:
            if len(sym) < 1 or len(sym) > 5:
                continue
            if sym in STOPWORDS:
                continue
            ticker_agg[sym]['mentions'] += 1
            ticker_agg[sym]['eng_score'] += eng
            ticker_agg[sym]['likes'] += metrics.get('like_count', 0) or 0
            ticker_agg[sym]['retweets'] += metrics.get('retweet_count', 0) or 0
            if len(ticker_agg[sym]['sample_texts']) < 2:
                ticker_agg[sym]['sample_texts'].append(text)

    # Filter: 2+ mentions
    filtered = {sym: d for sym, d in ticker_agg.items() if d['mentions'] >= 1}

    # Normalize and compute momo score
    max_eng = max((d['eng_score'] for d in filtered.values()), default=1) or 1
    max_mentions = max((d['mentions'] for d in filtered.values()), default=1) or 1

    results = []
    for sym, data in filtered.items():
        norm_eng = data['eng_score'] / max_eng * 60
        norm_mentions = data['mentions'] / max_mentions * 40
        momo_score = round(norm_eng + norm_mentions)

        results.append({
            'sym': sym,
            'mentions': data['mentions'],
            'eng_score': round(data['eng_score'], 1),
            'likes': data['likes'],
            'retweets': data['retweets'],
            'momoScore': momo_score,
            'sample_texts': data['sample_texts'],
        })

    results.sort(key=lambda x: x['momoScore'], reverse=True)

    return jsonify({
        'source': 'x_cashtags',
        'tickers': results[:50],
        'total_tweets_scanned': len(tweets),
        'fetchedAt': now_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
    })


@x_bp.route('/api/x/cashtags/<sym>')
def x_ticker(sym):
    """
    GET /api/x/cashtags/NVDA
    Returns X mention stats for a single ticker ($NVDA cashtag search).
    Requires X_BEARER_TOKEN env var.
    """
    sym = sym.upper()

    if not X_BEARER_TOKEN:
        return jsonify({'error': 'X_BEARER_TOKEN not configured'}), 503

    now_dt = datetime.now(timezone.utc)
    since = (now_dt - timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%SZ')

    params = {
        'query': f'${sym} lang:en -is:retweet',
        'max_results': 100,
        'tweet.fields': 'created_at,public_metrics,text',
        'start_time': since,
        'sort_order': 'relevancy',
    }

    try:
        resp = requests.get(
            f'{X_API_BASE}/tweets/search/recent',
            headers=_x_headers(),
            params=params,
            timeout=15,
        )
        if resp.status_code != 200:
            return jsonify({'error': f'X API returned {resp.status_code}'}), 502

        data = resp.json()
        tweets = data.get('data', [])

        total_likes = sum(t.get('public_metrics', {}).get('like_count', 0) for t in tweets)
        total_rts = sum(t.get('public_metrics', {}).get('retweet_count', 0) for t in tweets)
        eng_score = sum(_score_tweet(t.get('public_metrics', {}), t.get('created_at', ''), now_dt)
                        for t in tweets)

        return jsonify({
            'sym': sym,
            'mention_count': len(tweets),
            'total_likes': total_likes,
            'total_retweets': total_rts,
            'eng_score': round(eng_score, 1),
            'sample_tweets': [t.get('text', '')[:140] for t in tweets[:5]],
            'fetchedAt': now_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 502
