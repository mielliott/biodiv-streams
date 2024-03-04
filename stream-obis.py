import errno
import json
import sys
import time
import requests as rq

PAGE_SIZE = 1000
RETRY_DELAY_IN_SECONDS = 2
TIMEOUT_IN_SECONDS = 30

after = "-1"
while True:
    try:
        response = rq.get(f"https://api.obis.org/v3/occurrence?size={PAGE_SIZE}&after={after}", timeout=TIMEOUT_IN_SECONDS)
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

            if len(records) == 0:
                break
            
            after = records[-1]["id"]

        except ValueError as e:
            print(e, file=sys.stderr)
            continue
    except IOError as e:  
        if e.errno != errno.EPIPE:
            print(e, file=sys.stderr)
        break
