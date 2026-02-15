import os, datetime, requests
from flask import Flask, Response
from google.cloud import storage

app = Flask(__name__)

FEED_URL = os.environ.get(
    "FEED_URL",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"
)
BUCKET = os.environ["BUCKET_NAME"]
PREFIX = os.environ.get("PREFIX", "transit-weather-delays")

@app.get("/run")
def run():
    now = datetime.datetime.utcnow()
    dt = now.strftime("%Y-%m-%d")
    hr = now.strftime("%H")
    minute = now.strftime("%M")

    r = requests.get(FEED_URL, timeout=30)
    r.raise_for_status()
    data = r.content

    path = f"{PREFIX}/bronze/transit/dt={dt}/hr={hr}/feed_{minute}.pb"
    storage.Client().bucket(BUCKET).blob(path).upload_from_string(
        data, content_type="application/octet-stream"
    )

    return Response(
        f"Saved {len(data)} bytes to gs://{BUCKET}/{path}\n",
        mimetype="text/plain",
        status=200
    )

@app.get("/")
def health():
    return "OK\n", 200