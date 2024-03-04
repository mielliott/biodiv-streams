import errno
import json
import sys
import time
import requests as rq

PAGE_SIZE = 1000
RETRY_DELAY_IN_SECONDS = 2
TIMEOUT_IN_SECONDS = 30

def make_query(page, page_size):
    return {
        "rq": {},
        "limit": page_size,
        "offset": page
    }

page = 0
while True:
    try:
        query = make_query(page, PAGE_SIZE)
        response = rq.post("https://api.gbif.org/v1/occurrence/search", json=query, timeout=TIMEOUT_IN_SECONDS)
    except rq.exceptions.Timeout as e:
        print(e, file=sys.stderr)
        print(f"Retrying in {RETRY_DELAY_IN_SECONDS} seconds...", file=sys.stderr)
        continue

    if not response:
        print(f"Received unexpected response status code {response.status_code}", file=sys.stderr)
        time.sleep(RETRY_DELAY_IN_SECONDS)
        print(f"Retrying in {RETRY_DELAY_IN_SECONDS} seconds...", file=sys.stderr)
        continue

    try:
        try:
            response_data = response.json(strict=False)
            records = response_data["results"]
            for record in records:
                print(json.dumps(record), file=sys.stdout)

            # If this is the last page of records
            if response_data["endOfRecords"]:
                break

            page += 1
        except ValueError as e:
            print(e, file=sys.stderr)
            continue
    except IOError as e:  
        if e.errno != errno.EPIPE:
            print(e, file=sys.stderr)
        break
