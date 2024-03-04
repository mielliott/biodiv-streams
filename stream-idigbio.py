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
        response = rq.post("http://search.idigbio.org/v2/search/records/", json=query, timeout=TIMEOUT_IN_SECONDS)

        if not response:
            print(f"Received unexpected response status code {response.status_code}", file=sys.stderr)
            time.sleep(RETRY_DELAY_IN_SECONDS)
            print(f"Retrying in {RETRY_DELAY_IN_SECONDS} seconds...", file=sys.stderr)
            continue
    except rq.exceptions.Timeout as e:
        print(e, file=sys.stderr)
        print(f"Retrying in {RETRY_DELAY_IN_SECONDS} seconds...", file=sys.stderr)
        continue

    try:
        try:
            response_data = response.json(strict=False)
            records = response_data["items"]
            for record in records:
                print(json.dumps(record["data"]), file=sys.stdout)

            if response_data["itemCount"] <= query["limit"]:
                break

            page += 1
        except ValueError as e:
            print(e, file=sys.stderr)
    except IOError as e:  
        if e.errno != errno.EPIPE:
            print(e, file=sys.stderr)
        break
