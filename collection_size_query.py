# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = [
#     "httpx",
# ]
# ///

"""
Finds collections with a small number of items.

Usage:
    uv run --env-file ../.env ./collection_size_query.py

The `.env` sets the `SERVER_ROOT`, for production is `SERVER_ROOT="https://repository.library.brown.edu/"`.
The API url is public; the reason for it is to make dev testing easier.
"""

import logging
import os
import pprint
import sys
import time
from typing import Any

import httpx

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
)
log: logging.Logger = logging.getLogger(__name__)


## constants
SLEEP_TIME: float = 0.5  # seconds to sleep between requests
MIN_ITEMS_CONSIDERED_SMALL: int = 5  # min items for a collection to be considered small
MAX_ITEMS_CONSIDERED_SMALL: int = 50  # max items in a collection to consider it small
COLLECTIONS_PER_BATCH_SIZE: int = 100  # collections per batch
MAX_COLLECTIONS_TO_CHECK: int = 200  # max collections to check
COLLECTIONS_TO_GATHER_SIZE: int = 2  # number of collections to gather


def fetch_collections_batch(client: httpx.Client, server_root: str, start: int) -> list[dict[str, str | None]]:
    """
    Retrieves a single batch (page) of collection summaries from the collections API endpoint.
    The batch is determined by the `start` offset and COLLECTIONS_PER_BATCH_SIZE.
    Returns a list of dictionaries, each containing at least 'id' and possibly 'name' for a collection.
    Raises for HTTP errors.

    Called by find_small_collections() manager.
    """
    log.info(f'Fetching collections batch starting at {start}')
    url: str = f'{server_root}/api/collections/'
    params: dict[str, str] = {
        'rows': str(COLLECTIONS_PER_BATCH_SIZE),
        'start': str(start),
    }
    resp: httpx.Response = client.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data: dict[str, Any] = resp.json()
    collections_data: list[dict[str, str | None]] = data.get('collections', [])
    log.debug(f'collections_data, ``{pprint.pformat(collections_data)}``')
    return collections_data


def fetch_collection_item_count(
    client: httpx.Client,
    server_root: str,
    collection_id: str,
) -> int | None:
    """
    Submits a query to the search API for the given collection ID to retrieve the number of items in that collection.
    Returns the item count as an integer, or None if not present in the response.
    Raises for HTTP errors.

    Called by find_small_collections() manager.
    """
    q: str = f'rel_is_member_of_collection_ssim:"{collection_id}"'
    url: str = f'{server_root}/api/search/'
    params: dict[str, str] = {'q': q, 'rows': '0'}
    resp: httpx.Response = client.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data: dict[str, Any] = resp.json()
    item_count: int | None = data.get('response', {}).get('numFound')
    log.debug(f'item_count, ``{item_count}``')
    return item_count


def find_small_collections(server_root: str) -> list[dict[str, str | int | None]]:
    """
    Manager function.

    Iterates through batches of collections, checking up to `max_to_check` collections,
    and finds those with an item count between `min_items` and `max_items` (inclusive).
    Stops after finding more than `COLLECTIONS_TO_GATHER_SIZE` matches or reaching the check limit.
    For each qualifying collection, includes its ID, name, and item count in the result.
    Sleeps between item count requests to avoid overloading the server.

    Called by dundermain.
    """
    results: list[dict[str, str | int | None]] = []
    checked: int = 0
    start: int = 0
    done: bool = False

    with httpx.Client(timeout=10.0) as httpx_client:
        while not done and checked < MAX_COLLECTIONS_TO_CHECK and len(results) <= COLLECTIONS_TO_GATHER_SIZE:
            batch: list[dict[str, str | None]] = fetch_collections_batch(httpx_client, server_root, start)
            if not batch:
                log.info('No more collections returned by server.')
                break

            for entry in batch:
                summary: dict[str, str | None] = entry
                if len(results) > COLLECTIONS_TO_GATHER_SIZE or checked >= MAX_COLLECTIONS_TO_CHECK:
                    log.info('Enough small collections found or reached check limit, stopping.')
                    done = True
                    break

                collection_id: str = summary['id']
                name: str | None = summary.get('name')
                time.sleep(SLEEP_TIME)
                try:
                    count: int | None = fetch_collection_item_count(httpx_client, server_root, collection_id)
                    if count is None:
                        log.warning(f'No count returned for {collection_id}')
                        continue
                    log.info(f'Collection {collection_id}: {count} items')
                    if MIN_ITEMS_CONSIDERED_SMALL <= count <= MAX_ITEMS_CONSIDERED_SMALL:
                        result: dict[str, str | int | None] = {
                            'id': collection_id,
                            'name': name,
                            'count': count,
                        }
                        results.append(result)
                        log.info(f'Collection {collection_id} added to results (count: {count})')
                except Exception as e:
                    log.error(f'Error processing collection {collection_id}: {str(e)}')
                checked += 1

            start: int = start + COLLECTIONS_PER_BATCH_SIZE

    return results


def main() -> None:
    """
    Loads the SERVER_ROOT environment variable, calls `find_small_collections()` to get collections
    with a small number of items, and prints the ID, name, and item count for each found collection to stdout.
    """
    server_root: str = os.environ['SERVER_ROOT']
    small_collection: list[dict[str, str | int | None]] = find_small_collections(server_root)
    for info in small_collection:
        print(f'{info["id"]} ({info["name"]!r}) has {info["count"]} items')


if __name__ == '__main__':
    main()
