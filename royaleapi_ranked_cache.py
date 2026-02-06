#!/usr/bin/env python3
import argparse
import json
import math
import re
import time
from datetime import datetime, timezone
from urllib.parse import urlencode, urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup

BASE = "https://royaleapi.com"

BATTLE_SELECTORS = [
    "div.battle_list_battle",
    "div.battle_list_battle_container",
    "div.battle",
    "div.battle__container",
]

CARD_IMG_SELECTOR = "img.deck_card, img[data-card-key]"


def build_url(rank: int, lang: str, before: int | None) -> str:
    params = {"lang": lang, "rank": str(rank)}
    if before is not None:
        params["before"] = str(before)
    return f"{BASE}/decks/ranked?{urlencode(params)}"


def fetch_html(url: str) -> str | None:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    try:
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code != 200:
            return None
        return r.text
    except Exception:
        return None


def extract_next_before(soup: BeautifulSoup) -> int | None:
    # try any link containing before=
    for a in soup.select("a[href*='before=']"):
        href = a.get("href") or ""
        qs = parse_qs(urlparse(href).query)
        if "before" in qs:
            try:
                return int(qs["before"][0])
            except Exception:
                continue
    # fallback: regex search
    m = re.search(r"before=(\d+)", str(soup))
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None


def parse_deck_keys_from_imgs(imgs) -> list[str]:
    keys = []
    for img in imgs:
        key = (img.get("data-card-key") or "").strip()
        if not key:
            # fallback: attempt from img src
            src = (img.get("src") or "").strip()
            m = re.search(r"/cards/([a-z0-9\-]+)/", src)
            if m:
                key = m.group(1)
        if key:
            keys.append(key)
    return keys


def parse_matches_from_battle_el(el) -> list[dict]:
    imgs = el.select(CARD_IMG_SELECTOR)
    keys = parse_deck_keys_from_imgs(imgs)
    if len(keys) < 16:
        return []
    keys = keys[:16]
    return [{"winner": keys[:8], "loser": keys[8:16]}]


def parse_matches(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")

    battle_els = []
    for sel in BATTLE_SELECTORS:
        battle_els.extend(soup.select(sel))

    matches = []
    if battle_els:
        for el in battle_els:
            # require at least 16 card images
            if len(el.select(CARD_IMG_SELECTOR)) < 16:
                continue
            matches.extend(parse_matches_from_battle_el(el))
    else:
        # fallback: chunk all card images in page
        imgs = soup.select(CARD_IMG_SELECTOR)
        keys = parse_deck_keys_from_imgs(imgs)
        # group by 16
        for i in range(0, len(keys) - 15, 16):
            chunk = keys[i : i + 16]
            if len(chunk) < 16:
                break
            matches.append({"winner": chunk[:8], "loser": chunk[8:16]})

    return matches


def card_counts(matches: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for m in matches:
        for side in ("winner", "loser"):
            for k in m.get(side, []):
                if not k:
                    continue
                counts[k] = counts.get(k, 0) + 1
    return counts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=1000)
    ap.add_argument("--rank", type=int, default=1000)
    ap.add_argument("--lang", type=str, default="en")
    ap.add_argument("--delay", type=float, default=0.35)
    ap.add_argument("--out", type=str, default="scripts/royaleapi_ranked_cache.json")
    args = ap.parse_args()

    results: list[dict] = []
    before = None

    while len(results) < args.limit:
        url = build_url(rank=args.rank, lang=args.lang, before=before)
        html = fetch_html(url)
        if not html:
            break
        matches = parse_matches(html)
        if not matches:
            break
        for m in matches:
            if len(results) >= args.limit:
                break
            results.append(m)
        next_before = extract_next_before(BeautifulSoup(html, "html.parser"))
        if not next_before or next_before == before:
            break
        before = next_before
        time.sleep(args.delay)

    counts = card_counts(results)
    total_matches = len(results)
    top_cards = sorted(counts.items(), key=lambda x: (-x[1], x[0]))[:3]
    top_cards_out = [
        {
            "key": k,
            "count": v,
            "rate": round((v / total_matches) * 100, 2) if total_matches else 0.0,
        }
        for k, v in top_cards
    ]

    payload = {
        "fetchedAt": datetime.now(timezone.utc).isoformat(),
        "source": "royaleapi.com/decks/ranked",
        "rankGate": args.rank,
        "limit": args.limit,
        "totalMatches": total_matches,
        "topCards": top_cards_out,
        "matches": results,
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
