# Recovered from 2026 Sheridan DBAS31064 team project documentation.
# Academic prototype; requires configured Kafka/Hadoop environment.

from kafka import KafkaProducer
import sys
import time

if len(sys.argv) < 3:
    print("Usage: python3 p3producer.py <topic> <input_file>")
    sys.exit(1)

topic = sys.argv[1]
input_file = sys.argv[2]
try:
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda value: value.encode("utf-8")
    )
    with open(input_file, "r") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            columns = line.split()
            # Skip the header if the file contains one
            if columns[0].upper() == "LOG_ID":
                continue
            producer.send(topic, value=line)
            print("Reading sent:", line)
            time.sleep(0.15)
    # Tell the consumer that the file is finished
    producer.send(topic, value="END")
    producer.flush()
    print("All new meter readings were sent.")
except FileNotFoundError:
    print("Input file was not found:", input_file)
except Exception as e:
    print("Producer error:", e)
finally:
    try:
        producer.close()
    except:
        pass
