from kafka import KafkaProducer
import time
import csv

producer = KafkaProducer(
    bootstrap_servers = "localhost:9092",
    value_serializer = lambda v: str(v).encode("utf-8")
)

with open("../data/transformed_data.csv") as file:
    reader = csv.reader(file)
    next(reader)
    for row in reader:
        line = ','.join(row)
        producer.send('toll_data_topic', value=line)
        print(f"Sent: {line}")
        # time.sleep(1)

producer.flush()
producer.close()