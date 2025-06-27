# aqi-streaming-spark-de

Air Quality Streaming Analytics for Germany 🇩🇪
Real-time, region-aware analytics pipeline with Kafka, Spark Structured Streaming, and geospatial enrichment.

Overview
This project demonstrates a production-grade data engineering workflow:

Ingesting live air quality data from OpenAQ.

Streaming, parsing, and enriching events in real time with Apache Kafka and PySpark.

Assigning each air quality measurement to its German Bundesland (state) using geospatial lookup.

Performing rolling window aggregations to surface the regions with the best and worst air quality—live.

Why?
To showcase end-to-end data engineering skills—from data ingestion, streaming, and enrichment, to real-time analytics and geospatial processing. Suitable for technical interviews, portfolio reviews, and as a practical reference for scalable streaming solutions.

Features
Open-source data ingestion: Pulls latest air quality metrics (PM2.5, PM10, NO2) from OpenAQ for Germany.

Streaming pipeline: Uses Apache Kafka for robust data transport.

Real-time transformation: Spark Structured Streaming parses, explodes, and cleans JSON events.

Geospatial enrichment: Each event is mapped to its German Bundesland (state) by nearest centroid lookup (using GeoPandas-generated lookup table).

Windowed aggregation: 5-minute rolling averages per region and pollutant.

Streaming analytics output: Prints regional trends to the console, ready for downstream dashboards or storage.

Extensible: Easily adaptable to point-in-polygon mapping, external databases, or visualization frameworks.
