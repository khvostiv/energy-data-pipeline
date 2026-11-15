# Recovered from 2026 Sheridan DBAS31064 team project documentation.
# Academic prototype; requires configured Kafka/Hadoop environment.

from kafka import KafkaConsumer
import pyarrow as pa
from pyarrow import fs
import pyarrow.parquet as pq
import sys
import time

if len(sys.argv) < 2:
    print("Usage: python3 p3consumer.py <topic>")
    sys.exit(1)

topic = sys.argv[1]
batch_size = 50
bulk_by_folder = {}
try:
    consumer = KafkaConsumer(
        sys.argv[1],
        bootstrap_servers="localhost:9092",
        group_id=f"hdfs-group-phase3-{int(time.time())}",
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda v: v.decode("utf-8")
    )
    hdfs = fs.HadoopFileSystem(
        host="hd-master",
        port=9820
    )
    print("Consumer started. Waiting for readings...")
    for message in consumer:
        line = message.value.strip()
        # Write remaining records when the producer is done
        if line == "END":
            for hdfs_folder in bulk_by_folder:
                records = bulk_by_folder[hdfs_folder]
                if len(records) > 0:
                    hdfs.create_dir(
                        hdfs_folder,
                        recursive=True
                    )
                    table = pa.Table.from_pylist(records)
                    hdfs_file = (
                        f"{hdfs_folder}/"
                        f"kafka_batch_{int(time.time())}.parquet"
                    )
                    with hdfs.open_output_stream(hdfs_file) as file:
                        pq.write_table(
                            table,
                            file,
                            compression="snappy"
                        )
                    print(
                        len(records),
                        "remaining records written to",
                        hdfs_file
                    )
            consumer.commit()
            print("Consumer finished.")
            break
        columns = line.split()
        if len(columns) != 6:
            print("Invalid reading skipped:", line)
            continue
        try:
            log_id = int(columns[0])
            house_id = int(columns[1])
            condate = columns[2]
            conhour = columns[3]
            energy_reading = float(columns[4])
            flag = int(columns[5])
            year, month, day = condate.split("-")
            month = str(int(month))
            hdfs_folder = f"/energydata/{year}/{month}"
            if hdfs_folder not in bulk_by_folder:
                bulk_by_folder[hdfs_folder] = []
            bulk_by_folder[hdfs_folder].append({
                "LOG_ID": log_id,
                "HOUSE_ID": house_id,
                "CONDATE": condate,
                "CONHOUR": conhour,
                "ENERGY_READING": energy_reading,
                "FLAG": flag
            })
            if len(bulk_by_folder[hdfs_folder]) >= batch_size:
                records = bulk_by_folder[hdfs_folder]
                hdfs.create_dir(
                    hdfs_folder,
                    recursive=True
                )
                table = pa.Table.from_pylist(records)
                hdfs_file = (
                    f"{hdfs_folder}/"
                    f"kafka_batch_{int(time.time())}.parquet"
                )
                with hdfs.open_output_stream(hdfs_file) as file:
                    pq.write_table(
                        table,
                        file,
                        compression="snappy"
                    )
                consumer.commit()
                print(
                    len(records),
                    "records written to",
                    hdfs_file
                )
                records.clear()
        except ValueError:
            print("Invalid reading skipped:", line)
except Exception as e:
    print("Consumer error:", e)
finally:
    try:
        consumer.close()
    except:
        pass
