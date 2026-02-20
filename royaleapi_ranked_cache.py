#!/usr/bin/env python3
import argparse
import json
import math
import os
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
TEAM_SELECTORS = [
    "div.team-segment",
    "div.team_segment",
    "div.team",
]
BLOCK_TEXT_MARKERS = (
    "just a moment",
    "cf-browser-verification",
    "checking your browser before accessing",
)


def build_url(rank: int, lang: str, before: int | None) -> str:
    params = {"lang": lang, "rank": str(rank)}
    if before is not None:
        params["before"] = str(before)
    return f"{BASE}/decks/ranked?{urlencode(params)}"


def looks_like_block_page(html: str) -> bool:
    lower = html.lower()
    return any(marker in lower for marker in BLOCK_TEXT_MARKERS)


def fetch_html(url: str, *, max_attempts: int = 3, retry_delay: float = 1.0) -> str | None:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code != 200:
                print(
                    f"[fetch_html] status={r.status_code} attempt={attempt}/{max_attempts} url={url}"
                )
            elif looks_like_block_page(r.text):
                print(
                    f"[fetch_html] blocked_page_detected attempt={attempt}/{max_attempts} url={url}"
                )
            else:
                return r.text
        except Exception as e:
            print(
                f"[fetch_html] exception attempt={attempt}/{max_attempts} url={url} error={e}"
            )
        if attempt < max_attempts:
            time.sleep(retry_delay)
    return None


def load_existing_payload(path: str) -> dict | None:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, dict):
            return payload
    except Exception as e:
        print(f"[cache] failed_to_read_existing path={path} error={e}")
    return None


def count_payload_matches(payload: dict | None) -> int:
    if not payload or not isinstance(payload, dict):
        return 0
    matches = payload.get("matches")
    if isinstance(matches, list):
        return len(matches)
    total = payload.get("totalMatches")
    if isinstance(total, int):
        return total
    return 0


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


def _parse_deck_from_segment(seg) -> list[str]:
    imgs = seg.select(CARD_IMG_SELECTOR)
    return parse_deck_keys_from_imgs(imgs)


def parse_matches_from_battle_el(el) -> list[dict]:
    # 1) 팀 세그먼트 기반(왼쪽=승리, 오른쪽=패배)
    team_segments = []
    for sel in TEAM_SELECTORS:
        team_segments.extend(el.select(sel))

    if len(team_segments) >= 2:
        winner = _parse_deck_from_segment(team_segments[0])
        loser = _parse_deck_from_segment(team_segments[1])
        if len(winner) >= 8 and len(loser) >= 8:
            return [{"winner": winner[:8], "loser": loser[:8]}]

    # 2) fallback: 카드 이미지를 순서대로 8/8로 자름
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
    ap.add_argument(
        "--allow-empty-success",
        action="store_true",
        help="Exit 0 even when no new matches were fetched.",
    )
    args = ap.parse_args()

    results: list[dict] = []
    before = None
    existing_payload = load_existing_payload(args.out)
    existing_total = count_payload_matches(existing_payload)
    if existing_total > 0:
        print(f"[cache] existing_matches={existing_total} path={args.out}")

    while len(results) < args.limit:
        url = build_url(rank=args.rank, lang=args.lang, before=before)
        html = fetch_html(url)
        if not html:
            print(f"[crawl] stop:no_html url={url}")
            break
        matches = parse_matches(html)
        if not matches:
            print(f"[crawl] stop:no_matches url={url}")
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
    if total_matches == 0:
        print("[result] fetched_matches=0, keeping existing cache unchanged")
        if existing_total > 0:
            print(f"[result] fallback_to_existing matches={existing_total}")
        else:
            print("[result] no_existing_cache_available")
        return 0 if args.allow_empty_success else 2

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

    tmp_out = f"{args.out}.tmp"
    with open(tmp_out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_out, args.out)
    print(f"[result] wrote_matches={total_matches} path={args.out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
