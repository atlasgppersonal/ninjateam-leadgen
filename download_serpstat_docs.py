# download_serpstat_docs.py
# Fetch all Stoplight node JSONs for the Serpstat public docs and save into one JSON file.

import asyncio
import json
import os
import time
from typing import Any, Dict, List, Set

import httpx

# CONFIG - change if needed
STOPLIGHT_BASE = "https://stoplight.io/api/v1/"
PROJECT_ID = "cHJqOjI3NTI5NA"  # Serpstat project id observed earlier
OUTPUT_FILE = "serpstat_api_docs.json"
REQUEST_DELAY = 0.25  # seconds between node requests to be polite
HTTP_TIMEOUT = 20.0
MAX_RETRIES = 3


async def fetch_json(client: httpx.AsyncClient, url: str) -> Any:
    """GET JSON with retries. Returns parsed JSON or None."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = await client.get(url, timeout=HTTP_TIMEOUT)
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception:
                    # sometimes content-type is odd; attempt text->json parse
                    try:
                        return json.loads(r.text)
                    except Exception:
                        print(f"[WARN] {url} returned non-JSON payload.")
                        return None
            else:
                print(f"[WARN] {url} -> HTTP {r.status_code}")
        except Exception as e:
            print(f"[ERR] fetch {url} attempt {attempt}: {e}")
        await asyncio.sleep(0.5 * attempt)
    return None


def extract_slugs_and_ids(obj: Any, slugs: Set[str], ids: Set[str]) -> None:
    """Recursively walk JSON object collecting values of 'slug' and 'id' keys that look useful."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "slug" and isinstance(v, str) and v.strip():
                slugs.add(v.strip())
            elif k == "id" and isinstance(v, str) and v.strip():
                ids.add(v.strip())
            else:
                extract_slugs_and_ids(v, slugs, ids)
    elif isinstance(obj, list):
        for item in obj:
            extract_slugs_and_ids(item, slugs, ids)


async def find_table_of_contents(client: httpx.AsyncClient) -> Any:
    """Try multiple endpoints to discover the project's table-of-contents / nodes listing."""
    endpoints = [
        f"{STOPLIGHT_BASE}projects/{PROJECT_ID}/table-of-contents",
        f"{STOPLIGHT_BASE}projects/{PROJECT_ID}/table-of-contents?branch=main",
        f"{STOPLIGHT_BASE}projects/{PROJECT_ID}/nodes",
        f"{STOPLIGHT_BASE}nodes?project_ids[0]={PROJECT_ID}",
        f"{STOPLIGHT_BASE}projects/{PROJECT_ID}",
    ]
    for ep in endpoints:
        print(f"[INFO] Trying TOC endpoint: {ep}")
        data = await fetch_json(client, ep)
        if data:
            # Heuristic: if it contains at least one slug or looks like a list/dict with nodes, accept it
            slugs = set()
            ids = set()
            extract_slugs_and_ids(data, slugs, ids)
            if slugs or ids:
                print(f"[INFO] TOC endpoint {ep} returned {len(slugs)} slugs and {len(ids)} ids.")
                return data
            # if top-level looks like nodes list, accept it anyway
            if isinstance(data, list) and len(data) > 0:
                print(f"[INFO] TOC endpoint {ep} returned a non-empty list (len={len(data)}).")
                return data
            # otherwise keep trying
            print(f"[WARN] TOC endpoint {ep} returned content but no slugs/ids discovered.")
    return None


async def fetch_all_nodes():
    headers = {
        "User-Agent": "serpstat-docs-scraper/1.0 (+https://github.com/)",
        "Accept": "application/json, text/json, */*",
    }
    async with httpx.AsyncClient(headers=headers) as client:
        toc = await find_table_of_contents(client)
        if not toc:
            print("[ERROR] Could not fetch table-of-contents from Stoplight (no usable data). Aborting.")
            return

        # Extract possible slugs and ids
        slugs: Set[str] = set()
        ids: Set[str] = set()
        extract_slugs_and_ids(toc, slugs, ids)

        # If TOC is a list of node dicts with 'slug' or 'id' fields, we already captured them.
        print(f"[INFO] Collected {len(slugs)} slugs and {len(ids)} ids from TOC discovery.")

        # Build a canonical ordered list of identifiers to fetch.
        # Prefer slugs first (human-friendly), then ids that are not slugs.
        identifiers: List[str] = []
        # sort to have deterministic order
        for s in sorted(slugs):
            identifiers.append(s)
        for i in sorted(ids):
            if i not in slugs:
                identifiers.append(i)

        print(f"[INFO] Will attempt to fetch {len(identifiers)} node identifiers (slug/id).")

        results: List[Dict] = []
        seen_slugs: Set[str] = set()

        for idx, ident in enumerate(identifiers, start=1):
            print(f"[{idx}/{len(identifiers)}] Fetching node for '{ident}'...")
            # Try project-scoped node url first
            tried_urls = [
                f"{STOPLIGHT_BASE}projects/{PROJECT_ID}/nodes/{ident}",
                f"{STOPLIGHT_BASE}nodes/{ident}",
            ]
            node_data = None
            for url in tried_urls:
                node_data = await fetch_json(client, url)
                if node_data:
                    print(f"  [OK] fetched from {url}")
                    break
            if not node_data:
                print(f"  [WARN] failed to fetch node for '{ident}' from tried endpoints.")
                await asyncio.sleep(REQUEST_DELAY)
                continue

            # Deduplicate by slug if possible, otherwise by id
            node_slug = None
            if isinstance(node_data, dict):
                node_slug = node_data.get("slug") or node_data.get("id")
            if node_slug and node_slug in seen_slugs:
                print(f"  [SKIP] duplicate slug '{node_slug}'")
            else:
                if node_slug:
                    seen_slugs.add(node_slug)
                results.append(node_data)

            await asyncio.sleep(REQUEST_DELAY)

        # Final save
        os.makedirs(os.path.dirname(OUTPUT_FILE) or ".", exist_ok=True)
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump({"docs": results}, f, indent=2, ensure_ascii=False)

        print(f"[DONE] Fetched {len(results)} node documents. Saved to '{OUTPUT_FILE}'.")


def main():
    t0 = time.time()
    asyncio.run(fetch_all_nodes())
    print(f"[TIME] Completed in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
