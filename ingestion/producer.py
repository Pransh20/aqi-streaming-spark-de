import os
import time
import json
import logging
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load environment
load_dotenv()
API_KEY = os.getenv("OPENAQ_KEY")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9093")
TOPIC = "air_quality_de"

if not API_KEY:
    logger.error("OPENAQ_KEY is missing in .env")
    exit(1)

POLLUTANTS = {2: "pm25", 3: "pm10", 5: "no2"}

producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5
)

def fetch_latest_param(param_id):
    url = f"https://api.openaq.org/v3/parameters/{param_id}/latest"
    params = {"country": "DE", "limit": 5}
    resp = requests.get(url, headers={"X-API-Key": API_KEY}, params=params)
    logger.debug(f"API GET {url} status {resp.status_code}")
    resp.raise_for_status()
    return {
        "parameter": POLLUTANTS[param_id],
        "timestamp": int(time.time()),
        "results": resp.json().get("results", [])
    }

def main():
    logger.info("Starting main loop.")
    try:
        while True:
            for pid in POLLUTANTS:
                try:
                    logger.info(f"Fetching pollutant ID {pid}")
                    data = fetch_latest_param(pid)
                    logger.info(f"Fetched {len(data['results'])} records for {data['parameter']}")
                    future = producer.send(TOPIC, data)
                    logger.info(f"Sent data for {data['parameter']} to Kafka")
                except Exception as e:
                    logger.error(f"Error fetching or sending for pid {pid}: {str(e)}", exc_info=True)
                time.sleep(1)
            producer.flush()
            logger.info("Sleeping 30s before next batch")
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down.")

if __name__ == "__main__":
    main()

