import sys
import time
import requests as rq

PAGE_SIZE = 1000
RETRY_DELAY_IN_SECONDS = 2

def make_query(page, page_size):
    return {
        "rq": {},
        "limit": page_size,
        "offset": page
    }

page = 0
while page <= 1:
    query = make_query(page, PAGE_SIZE)
    response = rq.post("http://search.idigbio.org/v2/search/records/", json=query)
    if response.status_code != 200:
        time.sleep(RETRY_DELAY_IN_SECONDS)
        continue
    else:
        try:
            response_data = response.json(strict=False)
            records = response_data["items"]
            for record in records:
                print(record, file=sys.stdout)
        except ValueError as e:
            print(e, file=sys.stderr)
            continue

        page += 1

        # If this is the last page of records
        if response_data["itemCount"] <= query["limit"]:
            break
