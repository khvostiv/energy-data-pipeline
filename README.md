# Energy Data Pipeline

**Academic Big Data project | Hadoop, Kafka, Python, PySpark, Java MapReduce**

A four-phase group project for Sheridan College's DBAS31064 Big Data course (2026), completed by **Vasilina Khvostikova and Anita Kashani**. The project explores storing and processing historical household electricity readings and simulated incoming meter data.

## Technologies

- **Storage & infrastructure:** Hadoop HDFS, Linux, Bash
- **Batch processing:** Java Hadoop MapReduce, PySpark DataFrames and window functions
- **Streaming prototype:** Apache Kafka, Python (`kafka-python`)
- **File format:** Apache Parquet, PyArrow, Snappy compression
- **Security exercises:** HDFS permissions and encryption zones

## Project phases

| Phase | What we implemented |
| --- | --- |
| 1 — HDFS ingestion | Created year/month directories for energy and weather data; loaded historical text files with Hadoop CLI and Bash. |
| 2 — MapReduce | Java MapReduce jobs for electricity consumption analysis, with results documented in the course report. |
| 3 — Kafka ingestion | A Python producer published simulated meter readings to Kafka. A Python consumer validated records and wrote batches of 50 (plus the remainder) to year/month-partitioned Parquet files in HDFS. |
| 4 — PySpark & security | PySpark calculated actual energy consumption from cumulative readings using `lag()`, then produced average and maximum consumption summaries. Separate exercises configured HDFS access restrictions and weather-data encryption. |

## Architecture (course prototype)

```text
Historical energy/weather text ──> HDFS (/energydata, /weatherdata)
                                          |
                              +-----------+-----------+
                              |                       |
                              v                       v
                         Java MapReduce         PySpark analysis
                                                   (energy .txt)

New meter readings (sample text) --> Kafka producer --> Kafka topic
                                                          |
                                                          v
                                                    Kafka consumer
                                                          |
                                                          v
                                               Parquet/Snappy in HDFS
```

**Scope note:** These were course phases, not one fully integrated production deployment. The Phase 4 Spark jobs read historical `.txt` files; they did **not** yet consume the Parquet output created in Phase 3. The Kafka producer uses a finite, simulated file rather than real device feeds.

## Repository structure

```text
hadoop/       Example HDFS loading script
mapreduce/    Phase 2 Java MapReduce jobs (six screenshot-transcribed classes)
kafka/        Python Kafka producer and consumer
spark/        PySpark batch analysis scripts
docs/         Implementation notes and limitations
requirements.txt
```

## Running the prototype

This repository is **not a one-command deployment**. You need a configured Hadoop/HDFS environment, a Kafka broker, Spark, and the energy dataset in the documented HDFS layout.

1. Load the historical input data into `/energydata/<year>/<month>/`.
2. Configure a Kafka broker and create the `energy-readings` topic.
3. Start `kafka/p3consumer.py` **before** `kafka/p3producer.py` (the consumer uses latest-offset behavior for the classroom demo).
4. Run the PySpark jobs with `spark-submit` against the historical text data.

See [implementation notes](docs/PROJECT_NOTES.md) for important setup details and known limitations.

## Source provenance

The Python and PySpark scripts were **reconstructed from code printed in the original course reports**. Formatting was restored and some basic safety/portability adjustments may be present. They have **not been re-tested against the original classroom Hadoop server**. Phase 2 Java source has been transcribed from the original course-report screenshots into six `.java` files under `mapreduce/`. All recovered scripts require testing against a properly configured Hadoop environment.

The original reports also contain internal connection details and screenshots, so they are not published unredacted here. This is a collaborative academic project; both contributors are credited above.
