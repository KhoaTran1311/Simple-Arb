import argparse
import json
import logging
import sqlite3
from typing import Callable, Optional

import numpy as np
import requests
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

from gen_auth_headers import gen_kalshi_auth_headers

load_dotenv(verbose=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] -- %(name)s/%(funcName)s:%(lineno)d -- %(message)s",
    datefmt="%m/%d/%y %H:%M:%S",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("find_similar_markets")

KAL_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
POL_BASE_URL = "https://gamma-api.polymarket.com"
MODEL_NAME = "all-mpnet-base-v2"


def _fetch_paginated(
        base_url: str, path:str, limit: int,
        build_params: Callable[[Optional[str]], dict],
        get_next_cursor: Callable[[dict], str],
        auth_headers: dict = None,
):
    all_events = []

    cursor = None
    while True:
        params = build_params(cursor)
        resp = requests.get(base_url + path, headers=auth_headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        events = data.get("events", [])
        all_events.extend(events)
        cursor = get_next_cursor(data)
        if not cursor or len(events) < limit:
            break
    return all_events


def fetch_kalshi_events() -> list[dict]:
    limit = 200
    path = "/events"
    return _fetch_paginated(
        KAL_BASE_URL, path, limit=limit,
        build_params=lambda cursor: {
            "status": "open",
            "limit": limit,
            "with_nested_markets": True,
            **({"cursor": cursor} if cursor else {}),
        },
        get_next_cursor=lambda data: data.get("cursor"),
        auth_headers=gen_kalshi_auth_headers("GET", path)
    )


def fetch_polymarket_events() -> list[dict]:
    limit = 500
    return _fetch_paginated(
        POL_BASE_URL, "/events/keyset", limit=limit,
        build_params=lambda cursor: {
            "closed": False,
            "limit": limit,
            **({"after_cursor": cursor} if cursor else {}),
        },
        get_next_cursor=lambda data: data.get("next_cursor"),
    )


def sim_search(
    model: SentenceTransformer,
    collection1: list[str],
    collection2: list[str],
    k: int = 3,
    thres: float = 0.8,
) -> dict[int, list[tuple[int, float]]]:
    """Return top-k matches from collection2 for each item in collection1 above thres."""
    if not collection1 or not collection2:
        return {}

    k = min(k, len(collection2))

    col1_embds = model.encode(collection1, normalize_embeddings=True)
    col2_embds = model.encode(collection2, normalize_embeddings=True)

    similarities = col1_embds @ col2_embds.T

    topk_idx = np.argpartition(similarities, -k, axis=1)[:, -k:]
    topk_values = np.take_along_axis(similarities, topk_idx, axis=1)

    topk_thres_mask = topk_values >= thres
    collection1_res = np.where(topk_thres_mask.any(axis=1))[0]

    res = {}
    for idx in collection1_res:
        cur_idcs = topk_idx[idx][topk_thres_mask[idx]]
        cur_vals = topk_values[idx][topk_thres_mask[idx]]
        order = np.argsort(-cur_vals)
        res[int(idx)] = [(int(cur_idcs[j]), float(cur_vals[j])) for j in order]

    return res


def setup_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS similar_markets (
            kalshi_ticker       TEXT NOT NULL,
            poly_market_id      INTEGER NOT NULL,
            poly_asset_id       TEXT NOT NULL,
            similarity_score    REAL NOT NULL,
            kal_end_date        TEXT,
            kal_updated_time    TEXT,
            kal_volume          REAL,
            kal_volume_24h      REAL,
            poly_end_date       TEXT,
            poly_updated_time   TEXT,
            poly_volume         REAL,
            poly_volume_24h     REAL,
            PRIMARY KEY (kalshi_ticker, poly_asset_id)
        )
    """)
    conn.commit()
    return conn


def save_match(
    conn: sqlite3.Connection,
    kalshi_ticker: str,
    poly_asset_id: str,
    similarity_score: float,
    kal_record: dict,
    pol_market: dict,
):
    conn.execute(
        """
        INSERT OR REPLACE INTO similar_markets (
            kalshi_ticker, poly_market_id, poly_asset_id, similarity_score,
            kal_end_date, kal_updated_time, kal_volume, kal_volume_24h,
            poly_end_date, poly_updated_time, poly_volume, poly_volume_24h
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            kalshi_ticker,
            int(pol_market["id"]),
            poly_asset_id,
            similarity_score,
            kal_record.get("expected_expiration_time"),
            kal_record.get("updated_time"),
            float(kal_record["volume_fp"]) if kal_record.get("volume_fp") else None,
            float(kal_record["volume_24h_fp"])
            if kal_record.get("volume_24h_fp")
            else None,
            pol_market.get("endDate"),
            pol_market.get("updatedAt"),
            float(pol_market["volume"]) if pol_market.get("volume") else None,
            float(pol_market["volume24hr"]) if pol_market.get("volume24hr") else None,
        ),
    )
    conn.commit()


def main(
    event_k: int, event_thres: float, market_k: int, market_thres: float, db_path: str
):
    conn = setup_db(db_path)
    logger.info(f"Database ready at {db_path}")

    logger.info("Fetching Kalshi events...")
    kal_events = fetch_kalshi_events()
    logger.info(f"Fetched {len(kal_events)} Kalshi events")

    logger.info("Fetching Polymarket events...")
    pol_events = fetch_polymarket_events()
    logger.info(f"Fetched {len(pol_events)} Polymarket events")

    logger.info(f"Loading model: {MODEL_NAME}")
    model = SentenceTransformer(MODEL_NAME, trust_remote_code=True)

    kal_titles = [e["title"] for e in kal_events]
    pol_titles = [e["title"] for e in pol_events]

    logger.info(f"Running event-level similarity (k={event_k}, thres={event_thres})...")
    similar_events = sim_search(
        model, pol_titles, kal_titles, k=event_k, thres=event_thres
    )
    logger.info(
        f"Found {len(similar_events)} Polymarket events with similar Kalshi counterparts"
    )

    for pol_idx, matches in similar_events.items():
        pol_markets = pol_events[pol_idx].get("markets", [])
        if not pol_markets:
            continue

        kal_market_records = []
        for kal_idx, event_score in matches:
            event = kal_events[kal_idx]
            for market_idx, market in enumerate(event.get("markets", [])):
                if market.get("status", "") != "active":
                    continue

                kal_market_records.append(
                    {
                        "event_idx": kal_idx,
                        "event_title": event["title"],
                        "event_score": event_score,
                        "market_idx": market_idx,
                        "title": market.get("title", ""),
                        "rules_primary": market.get("rules_primary", ""),
                        "rules_secondary": market.get("rules_secondary", ""),
                        "ticker": market.get("ticker", ""),
                        "expected_expiration_time": market.get(
                            "expected_expiration_time"
                        ),
                        "updated_time": market.get("updated_time"),
                        "volume_fp": market.get("volume_fp"),
                        "volume_24h_fp": market.get("volume_24h_fp"),
                    }
                )

        if not kal_market_records:
            continue

        active_pol_markets = [m for m in pol_markets if m["active"] and not m["closed"]]
        pol_markets_desc = [
            f"{m['question']}\n\n{m['description']}" for m in active_pol_markets
        ]
        kal_markets_desc = [
            f"{r['title']}\n\n{r['rules_primary']}\n\n{r['rules_secondary']}"
            for r in kal_market_records
        ]

        similar_markets = sim_search(
            model, pol_markets_desc, kal_markets_desc, k=market_k, thres=market_thres
        )

        for pol_m_idx, kal_sim in similar_markets.items():
            pol_market = active_pol_markets[pol_m_idx]
            clob_ids = json.loads(pol_market.get("clobTokenIds", "[]"))
            pol_asset_id = clob_ids[0] if clob_ids else "N/A"

            best_kal_m_idx, best_score = kal_sim[0]
            best_kal_title = kal_market_records[best_kal_m_idx]["title"]
            logger.info(
                f"Found {len(kal_sim)} similar market(s), best score={best_score:.3f} "
                f"('{best_kal_title}') for '{pol_market['question']}'"
            )
            for kal_m_idx, score in kal_sim:
                rec = kal_market_records[kal_m_idx]
                save_match(conn, rec["ticker"], pol_asset_id, score, rec, pol_market)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Find similar markets between Kalshi and Polymarket using semantic similarity"
    )
    parser.add_argument(
        "--event_k",
        type=int,
        default=3,
        help="Top-k Kalshi events to consider per Polymarket event (default: 3)",
    )
    parser.add_argument(
        "--event_thres",
        type=float,
        default=0.75,
        help="Cosine similarity threshold for event-level matching (default: 0.75)",
    )
    parser.add_argument(
        "--market_k",
        type=int,
        default=3,
        help="Top-k Kalshi markets to consider per Polymarket market (default: 3)",
    )
    parser.add_argument(
        "--market_thres",
        type=float,
        default=0.5,
        help="Cosine similarity threshold for market-level matching (default: 0.5)",
    )
    parser.add_argument(
        "--db",
        type=str,
        default="similar_markets.db",
        help="Path to SQLite database for saving matches (default: similar_markets.db)",
    )

    args = parser.parse_args()
    main(args.event_k, args.event_thres, args.market_k, args.market_thres, args.db)
