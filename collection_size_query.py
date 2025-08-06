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
import sys
import time
from typing import TypedDict

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


## constants
SLEEP_TIME = 0.5  # seconds to sleep between requests
MIN_ITEMS_CONSIDERED_SMALL = 5  # min items for a collection to be considered small
MAX_ITEMS_CONSIDERED_SMALL = 50  # max items in a collection to consider it small
COLLECTIONS_PER_BATCH_SIZE = 100  # collections per batch
MAX_COLLECTIONS_TO_CHECK = 200  # max collections to check
GATHER_SIZE = 3  # number of collections to gather


class CollectionSummary(TypedDict):
    id: str
    name: str | None


class CollectionInfo(TypedDict):
    id: str
    name: str | None
    count: int


def fetch_collections_batch(
    client: httpx.Client, server_root: str, start: int
) -> list[CollectionSummary]:
    """
    Retrieves a single batch (page) of collection summaries from the collections API endpoint.
    The batch is determined by the `start` offset and COLLECTIONS_PER_BATCH_SIZE.
    Returns a list of dictionaries, each containing at least 'id' and possibly 'name' for a collection.
    Raises for HTTP errors.
    """
    logger.info(f"Fetching collections batch starting at {start}")
    url = f"{server_root}/api/collections/"
    params = {"rows": COLLECTIONS_PER_BATCH_SIZE, "start": start}
    resp = client.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return data.get("collections", [])


def fetch_collection_item_count(
    client: httpx.Client,
    server_root: str,
    collection_id: str,
) -> int | None:
    """
    Submits a query to the search API for the given collection ID to retrieve the number of items in that collection.
    Returns the item count as an integer, or None if not present in the response.
    Raises for HTTP errors.
    """
    q = f'rel_is_member_of_collection_ssim:"{collection_id}"'
    url = f"{server_root}/api/search/"
    params = {"q": q, "rows": 0}
    resp = client.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return data.get("response", {}).get("numFound")


def find_small_collections(server_root: str) -> list[CollectionInfo]:
    """
    Iterates through batches of collections, checking up to `max_to_check` collections,
    and finds those with an item count between `min_items` and `max_items` (inclusive).
    Stops after finding more than `GATHER_SIZE` matches or reaching the check limit.
    For each qualifying collection, includes its ID, name, and item count in the result.
    Sleeps between item count requests to avoid overloading the server.
    """
    results: list[CollectionInfo] = []
    checked = 0
    start = 0

    with httpx.Client(timeout=10.0) as httpx_client:
        while checked < MAX_COLLECTIONS_TO_CHECK and len(results) <= GATHER_SIZE:
            batch = fetch_collections_batch(httpx_client, server_root, start)
            if not batch:
                logger.info("No more collections returned by server.")
                break

            for summary in batch:
                if len(results) > GATHER_SIZE or checked >= MAX_COLLECTIONS_TO_CHECK:
                    logger.info(
                        "Enough small collections found or reached check limit, stopping."
                    )
                    return results

                collection_id = summary["id"]
                name = summary.get("name")
                time.sleep(SLEEP_TIME)
                try:
                    count = fetch_collection_item_count(
                        httpx_client, server_root, collection_id
                    )
                    if count is None:
                        logger.warning(f"No count returned for {collection_id}")
                        continue
                    logger.info(f"Collection {collection_id}: {count} items")
                    if (
                        MIN_ITEMS_CONSIDERED_SMALL
                        <= count
                        <= MAX_ITEMS_CONSIDERED_SMALL
                    ):
                        result = {"id": collection_id, "name": name, "count": count}
                        results.append(result)
                        logger.info(
                            f"Collection {collection_id} added to results (count: {count})"
                        )
                except Exception as e:
                    logger.error(
                        f"Error processing collection {collection_id}: {str(e)}"
                    )
                checked += 1

            start += COLLECTIONS_PER_BATCH_SIZE

    return results


def main() -> None:
    """
    Loads the SERVER_ROOT environment variable, calls `find_small_collections()` to get collections
    with a small number of items, and prints the ID, name, and item count for each found collection to stdout.
    """
    server_root = os.environ["SERVER_ROOT"]
    small = find_small_collections(server_root)
    for info in small:
        print(f"{info['id']} ({info['name']!r}) has {info['count']} items")


if __name__ == "__main__":
    main()
