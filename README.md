# Air Quality Streaming Analytics for Germany 🇩🇪

**Real-time, region-aware analytics pipeline with Kafka, Spark Structured Streaming, and geospatial enrichment.**

---

## Overview

This project demonstrates a production-grade data engineering workflow:

- Ingests live air quality data from [OpenAQ](https://openaq.org/).
- Streams, parses, and enriches events in real time with Apache Kafka and PySpark.
- Assigns each air quality measurement to its German Bundesland (state) using geospatial lookup.
- Performs rolling window aggregations to surface the regions with the best and worst air quality—**live**.

**Why?**  
To showcase end-to-end data engineering skills—from data ingestion, streaming, and enrichment, to real-time analytics and geospatial processing. Suitable for technical interviews, portfolio reviews, and as a practical reference for scalable streaming solutions.

---

## Features

- **Open-source data ingestion:** Pulls latest air quality metrics (PM2.5, PM10, NO2) from OpenAQ for Germany.
- **Streaming pipeline:** Uses Apache Kafka for robust data transport.
- **Real-time transformation:** Spark Structured Streaming parses, explodes, and cleans JSON events.
- **Geospatial enrichment:** Each event is mapped to its German Bundesland (state) by nearest centroid lookup (using a GeoPandas-generated lookup table).
- **Windowed aggregation:** 5-minute rolling averages per region and pollutant.
- **Streaming analytics output:** Prints regional trends to the console, ready for downstream dashboards or storage.
- **Extensible:** Easily adaptable to point-in-polygon mapping, external databases, or visualization frameworks.

---

## Architecture

```text
[OpenAQ API] --> [Kafka Producer] --> [Kafka Topic] --> [Spark Structured Streaming]
                                                      |
                                                      +--> [Bundesland Geospatial Enrichment]
                                                      +--> [Rolling Window Aggregation]
                                                      +--> [Console Output | Parquet | DB]

```


## Getting Started

### 1. Clone this repository

```bash
git clone https://github.com/YOUR_USERNAME/aqi-streaming-spark-de.git
cd aqi-streaming-spark-de



### 2. Install dependencies

- Python 3.8+ (recommended: use `venv` or `conda`)
- Java 17+ (required for Spark 4.x)
- [Kafka](https://kafka.apache.org/quickstart) and [Spark](https://spark.apache.org/downloads.html)
- Install Python packages:

```bash
pip install -r requirements.txt


### 3. Set up .env

Create a `.env` file in the root directory:

OPENAQ_KEY=your_openaq_api_key
KAFKA_BOOTSTRAP=localhost:9093


Get an OpenAQ API key from [OpenAQ](https://docs.openaq.org/docs/getting-started).


### 4. Start Kafka (Docker example)

```bash
docker compose up -d kafka zookeeper


### 5. Run the Kafka Producer

```bash
python ingestion/producer.py


### 6. Prepare Region Lookup Table

Download German Bundesländer geojson:

```bash
curl -o germany_states.geojson \
  https://raw.githubusercontent.com/isellsoap/deutschlandGeoJSON/main/2_bundeslaender/2_hoch.geo.json


Generate centroids CSV:
import geopandas as gpd
gdf = gpd.read_file("germany_states.geojson")
gdf = gdf.to_crs(25832)  # Projected CRS for accurate centroids
gdf = gdf.rename(columns={"name": "bundesland"})
gdf["lat"] = gdf.geometry.centroid.y
gdf["lon"] = gdf.geometry.centroid.x
gdf[["bundesland", "lat", "lon"]].to_csv("region_lookup.csv", index=False)



### 7. Run Spark Structured Streaming Job

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  streaming/spark_job.py


This will:

Parse and explode messages from Kafka
Assign each event to a Bundesland
Aggregate per region and pollutant over rolling 5-minute windows
Print the output to console (and ready for downstream sinks!)

## Example Output
+------------------------------------------+------------------+----------+------------------+
|window |bundesland |parameter |avg_value |
+------------------------------------------+------------------+----------+------------------+
|{2025-06-27 20:30:00, 2025-06-27 20:35:00}|Bayern |pm25 | 8.33 |
|{2025-06-27 20:30:00, 2025-06-27 20:35:00}|Berlin |no2 | 17.80 |
...
+------------------------------------------+------------------+----------+------------------+

## Limitations and Extensibility

- **Centroid mapping is fast and robust for demo/portfolio; production systems should use point-in-polygon with Sedona or GeoPandas.**
- **Add streaming sinks:** Results can be written to Parquet, Delta Lake, Postgres, ClickHouse, or visualized live.
- **Dashboards:** Next step—visualization with Streamlit, Dash, or Grafana.

## File Structure

```text
aqi-streaming-spark-de/
├── ingestion/
│   └── producer.py              # Kafka producer for OpenAQ data
├── streaming/
│   └── spark_job.py             # Spark Structured Streaming ETL
├── region_lookup.csv            # Generated Bundesland centroid lookup
├── germany_states.geojson       # Downloaded state boundaries
├── requirements.txt             # Python dependencies
└── README.md                    # This file

## References

- [OpenAQ API](https://docs.openaq.org/docs/getting-started)
- [Apache Kafka](https://kafka.apache.org/)
- [Apache Spark](https://spark.apache.org/)
- [GeoJSON Bundesländer (geojson.de)](https://geojson.de)
- [PySpark Pandas UDFs](https://spark.apache.org/docs/latest/api/python/user_guide/sql/pandas_on_spark/groupby.html)


## Contact / License

MIT License.  
Project by Pransh Arora.  
Questions, feedback, or ideas? pransh.arora@gmail.com

## Next Steps

Visualize these results live in Streamlit or your favorite dashboarding tool!
