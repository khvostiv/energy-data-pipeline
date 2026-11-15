# Implementation notes

The following summarizes the original four 2026 course phases, based on the team reports.

## Phase 1 — HDFS file organization
- Historical energy and weather files were stored in Hadoop HDFS.
- The directory layout was organized by year and month, e.g. `/energydata/2015/5/`.
- Bash and Hadoop CLI commands were used to create directories and upload text files.
- The example script in `hadoop/` is illustrative; source dataset files are **not included**.

## Phase 2 — Java MapReduce
- Java MapReduce jobs analyzed electricity consumption, including maximum and average calculations.
- The six Java classes were transcribed from screenshots in the Phase 2 report and committed under `mapreduce/average-consumption/` and `mapreduce/max-electricity/`.
- Screenshot transcription preserves the course prototype logic; these classes have not yet been compiled or tested against the original Hadoop environment.
- **Important distinction:** the original Phase 2 average reducer computes the mean of the meter reading values by house/date. The later Phase 4 Spark implementation instead computes per-interval usage from cumulative readings before aggregation. This difference has not been silently corrected in the Java code.

## Phase 3 — Kafka to HDFS
- Input: 60 simulated readings with `LOG_ID HOUSE_ID CONDATE CONHOUR ENERGY_READING FLAG`.
- `p3producer.py` reads the input file and publishes each reading to the `energy-readings` topic.
- `p3consumer.py` groups records by year/month, writes batches of 50 through PyArrow to Parquet with Snappy compression, then flushes the remaining records on an `END` message.
- The classroom broker was `localhost:9092`; the consumer connected to a configured HDFS NameNode.
- The classroom example used a new consumer group and `auto_offset_reset=latest`, so the consumer must be running before the producer sends messages.
- PyArrow's Hadoop filesystem connector needs access to Hadoop's Java classes (e.g. `export CLASSPATH=$(hadoop classpath --glob)`).

**Known limitations:** Uses simulated file input, a finite `END` sentinel rather than a continuously running service, basic validation, timestamp-based output filenames, no duplicate-event handling, and possible message loss if the consumer is not already listening.

## Phase 4 — PySpark and Hadoop security
- The Spark scripts read `hdfs:///energydata/*/*/*.txt`.
- Data contained **cumulative meter readings**, so interval consumption was computed as current reading minus the preceding reading for the same house/date, ordered by timestamp.
- Spark window functions (`lag`) and aggregations produced average daily, maximum hourly, maximum daily, and total-consumption summaries.
- Separate security work demonstrated HDFS ownership/permission restrictions and a weather-data encryption zone via Hadoop KMS.

**Known limitations:** The Spark scripts process the historical text format; the Kafka-produced Parquet is not currently merged into the Spark jobs. Security configuration is environment-specific and is deliberately not published as a runnable server configuration.

## Reproduction and safety
- Install and configure Hadoop, Spark, Kafka and the Python dependencies.
- Adjust host, port, and filesystem locations for **your own environment**.
- No passwords, private SSH addresses, KMS settings, or original course VM configuration should be committed.
- The code reconstructed from the reports is provided as a **portfolio reference**, not as a verified production-ready system.
