import asyncio
import json
import random
import string
from typing import List, TypedDict

import httpx
from loguru import logger as log


class LocationData(TypedDict):
    """result dataclass for tripadvisor location data"""

    localizedName: str
    url: str
    HOTELS_URL: str
    ATTRACTIONS_URL: str
    RESTAURANTS_URL: str
    placeType: str
    latitude: float
    longitude: float


async def scrape_location_data(query: str, client: httpx.AsyncClient) -> List[LocationData]:
    """
    scrape search location data from a given query.
    e.g. "New York" will return us TripAdvisor's location details for this query
    """
    log.info(f"scraping location data: {query}")
    # the graphql payload that defines our search
    # note: that changing values outside of expected ranges can block the web scraper
    payload = [
            {
                "variables": {
                    "request": {
                        "query": query,
                        "limit": 10,
                        "scope": "WORLDWIDE",
                        "locale": "en-US",
                        "scopeGeoId": 1,
                        "searchCenter": None,
                        # note: here you can expand to search for differents.
                        "types": [
                            "LOCATION",
                            # "QUERY_SUGGESTION",
                            # "RESCUE_RESULT"
                        ],
                        "locationTypes": [
                            "GEO",
                            "AIRPORT",
                            "ACCOMMODATION",
                            "ATTRACTION",
                            "ATTRACTION_PRODUCT",
                            "EATERY",
                            "NEIGHBORHOOD",
                            "AIRLINE",
                            "SHOPPING",
                            "UNIVERSITY",
                            "GENERAL_HOSPITAL",
                            "PORT",
                            "FERRY",
                            "CORPORATION",
                            "VACATION_RENTAL",
                            "SHIP",
                            "CRUISE_LINE",
                            "CAR_RENTAL_OFFICE",
                        ],
                        "userId": None,
                        "context": {},
                        "enabledFeatures": ["articles"],
                        "includeRecent": True,
                    }
                },
                # Every graphql query has a query ID that doesn't change often:
                "query": "84b17ed122fbdbd4",
                "extensions": {"preRegisteredQueryId": "84b17ed122fbdbd4"},
            }
        ]

    # we need to generate a random request ID for this request to succeed
    random_request_id = "".join(
        random.choice(string.ascii_lowercase + string.digits) for i in range(180)
    )
    headers = {
        "X-Requested-By": random_request_id,
        "Referer": "https://www.tripadvisor.com/Hotels",
        "Origin": "https://www.tripadvisor.com",
    }
    result = await client.post(
        url="https://www.tripadvisor.com/data/graphql/ids",
        json=payload,
        headers=headers,
    )
    data = json.loads(result.content)
    results = data[0]["data"]["Typeahead_autocomplete"]["results"]
    results = [r["details"] for r in results]  # strip metadata
    log.info(f"found {len(results)} results")
    return results

# To avoid being instantly blocked we'll be using request headers that
# mimic Chrome browser on Windows
BASE_HEADERS = {
    "authority": "www.tripadvisor.com",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "accept-language": "en-US;en;q=0.9",
    "accept-encoding": "gzip, deflate, br",
}
# start HTTP session client with our headers and HTTP2
client = httpx.AsyncClient(
    http2=True,  # http2 connections are significantly less likely to get blocked
    headers=BASE_HEADERS,
    timeout=httpx.Timeout(150.0),
    limits=httpx.Limits(max_connections=5),
)


async def run():
    result = await scrape_location_data("Malta", client)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    asyncio.run(run())