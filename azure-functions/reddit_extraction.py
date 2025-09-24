'''This Azure Function authenticates with Reddit, fetches posts from several Audi-related subreddits, 
and structures key fields (title, author, score, etc.). 

It then serializes the data as JSON and 
uploads it into a date-based folder in Azure Blob Storage.'''

import azure.functions as func
import logging
import os
import requests
import requests.auth
import json
from azure.storage.blob import BlobServiceClient
from datetime import datetime

app = func.FunctionApp()

@app.schedule(schedule="0 0 0 1 * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def audi_reddit_etl(mytimer: func.TimerRequest) -> None:
    logging.info('Audi Reddit ETL timer function triggered.')

    # Load credentials from environment variables
    CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
    CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
    USERNAME = os.environ.get("REDDIT_USERNAME")
    PASSWORD = os.environ.get("REDDIT_PASSWORD")
    USER_AGENT = os.environ.get("REDDIT_USER_AGENT", "AudiPipeline/0.0.1")
    BLOB_CONN_STR = os.environ.get("BLOB_CONN_STR")
    BLOB_CONTAINER = os.environ.get("BLOB_CONTAINER", "reddit-data")

    # Authenticate with Reddit
    client_auth = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)
    post_data = {"grant_type": "password", "username": USERNAME, "password": PASSWORD}
    headers = {"User-Agent": USER_AGENT}
    token_res = requests.post(
        "https://www.reddit.com/api/v1/access_token",
        data=post_data,
        headers=headers,
        auth=client_auth
    )
    if token_res.status_code != 200:
        logging.error(f"Reddit auth failed: {token_res.text}, {token_res.status_code}")
        return
    token_id = token_res.json()["access_token"]

    # OAuth headers
    headers_get = {
        "User-Agent": USER_AGENT,
        "Authorization": f"Bearer {token_id}"
    }

    # Fetch posts function
    def fetch_posts(endpoint, max_posts=1000):
        all_posts = []
        after = None
        while len(all_posts) < max_posts:
            params = {"limit": 100, "after": after}
            resp = requests.get("https://oauth.reddit.com" + endpoint, headers=headers_get, params=params)
            if resp.status_code != 200:
                break
            data = resp.json()["data"]
            children = data.get("children", [])
            if not children:
                break
            for post in children:
                p = post["data"]
                all_posts.append({
                    "id": p.get("id"),
                    "subreddit": p.get("subreddit"),
                    "title": p.get("title"),
                    "selftext": p.get("selftext"),
                    "author": p.get("author"),
                    "created_utc": p.get("created_utc"),
                    "score": p.get("score"),
                    "num_comments": p.get("num_comments"),
                    "upvote_ratio": p.get("upvote_ratio"),
                    "link_flair_text": p.get("link_flair_text"),
                    "permalink": "https://reddit.com" + p.get("permalink", ""),
                    "url": p.get("url"),
                    "subreddit_subscribers": p.get("subreddit_subscribers"),
                    "source": endpoint
                })
            after = data.get("after")
            if after is None:
                break
        return all_posts

    # Subreddit sources
    sources = {
        "Audi_new": "/r/Audi/new",
        "Audi_hot": "/r/Audi/hot",
        "Audi_top": "/r/Audi/top",
        "Audi_best": "/r/Audi/best",
        "AudiA4_best": "/r/AudiA4/best",
        "AudiS4_best": "/r/AudiS4/best",
        "AudiS5_best": "/r/audis5/best",
        "AudiQ6_best": "/r/AudiQ6/best",
        "AudiQ5_best": "/r/AudiQ5/best",
        "AudiA6_best": "/r/AudiA6/best",
        "AudiR8_best": "/r/audir8/best",
        "AudiQ7_best": "/r/AudiQ7/best",
        "AudiQ3_best": "/r/AudiQ3/best",
        "AudiA3_best": "/r/AudiA3/best",
    }

    all_data = []
    for source, endpoint in sources.items():
        posts = fetch_posts(endpoint, max_posts=100)
        for p in posts:
            p["source"] = source
        all_data.extend(posts)

    if not all_data:
        logging.info("No data fetched from Reddit.")
        return

    # Serialize data to JSON
    json_data = json.dumps(all_data, ensure_ascii=False, indent=2)

    # Upload to Azure Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    container_client = blob_service_client.get_container_client(BLOB_CONTAINER)

    blob_name = f"bronze/{datetime.utcnow().strftime('%Y/%m')}/audi_reddit.json"
    container_client.upload_blob(name=blob_name, data=json_data, overwrite=True)

    logging.info(f"Fetched {len(all_data)} posts and uploaded to Azure Blob Storage as {blob_name}.")    
