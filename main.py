import re
import json
import time
import base64
import sys
import os

from flask import Flask, request
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery

app = Flask(__name__)
url = "https://boards.4chan.org/{}/catalog"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "C:\\Users\\Almighty Yoyo\\PycharmProjects\\4chan-scraper\\keys\\credentials.json"
bq_client = bigquery.Client()
dataset_ref = bq_client.dataset('4chan')


def get_threads(response):
    soup = BeautifulSoup(response.content, 'lxml')
    scripts = soup.findAll('script', attrs={'type': 'text/javascript'})
    scripts = sorted(scripts, key=lambda x: len(x.text), reverse=True)
    raw_script = scripts[0].text
    threads = re.search(r'\{\"threads\".*\}', raw_script)
    if threads:
        threads = json.loads(threads.group(0))
        return threads
    else:
        raise AttributeError("no script")


def transform(thread_id, thread_json, fetch_time):
    transformed = {"thread_id": thread_id,
                   "creation_time": thread_json["date"],
                   "attached_file": thread_json["file"],
                   "replies": thread_json["r"],
                   "image_replies": thread_json["i"],
                   "b": thread_json["b"],
                   "author": thread_json["author"],
                   "imgurl": thread_json["imgurl"] if "imgurl" in thread_json.keys() else None,
                   "thumbnail_width": thread_json["tn_w"] if "tn_w" in thread_json.keys() else None,
                   "thumbnail_height": thread_json["tn_h"] if "tn_h" in thread_json.keys() else None,
                   "sub": thread_json["sub"],
                   "teaser": thread_json["teaser"],
                   "fetch_time": fetch_time
                   }
    return transformed


@app.route('/', methods=['POST'])
def scrap_board():
    envelope = request.get_json()
    if not envelope:
        msg = 'no Pub/Sub message received'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400

    if not isinstance(envelope, dict) or 'message' not in envelope:
        msg = 'invalid Pub/Sub message format'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400

    pubsub_message = envelope['message']

    if isinstance(pubsub_message, dict) and 'data' in pubsub_message:
        board = base64.b64decode(pubsub_message['data']).decode('utf-8').strip()
        try:
            scrape(board)
        except Exception as e:
            msg = 'Error occured when scraping board'
            print(f'error: {msg}')
            print(f'error: {e}')
            print(f'error: {type(e)}')
            return f'Internal Server Error: {msg}', 500
    else:
        msg = 'board name not understood'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400

    # Flush the stdout to avoid log buffering.
    sys.stdout.flush()
    return ('', 204)


def scrape(board):
    target = url.format(board)
    table_ref = dataset_ref.table(board)
    table = bq_client.get_table(table_ref)
    response = requests.get(target)
    if response.status_code < 400:
        fetch_time = int(time.time())
        threads = get_threads(response)
        rows = []
        for thread in threads['threads']:
            transformed = transform(thread, threads['threads'][thread], fetch_time)
            rows.append(transformed)
        errors = bq_client.insert_rows(table, rows)  # API request
        print("errors: {}".format(errors))
        assert errors == []
    else:
        print("error'd when fetching {}".format(target))


if __name__ == '__main__':
    PORT = int(os.getenv('PORT')) if os.getenv('PORT') else 8080
    app.run(host='127.0.0.1', port=PORT)
