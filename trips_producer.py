from kafka import KafkaProducer

import csv
import json
import time

BROCKER_SERVERS = ["localhost:9092"]
TOPIC_NAME = "taxi_pricing_topic"

producer = KafkaProducer(bootstrap_servers=BROCKER_SERVERS) # send할때 topic name 넣어도 됨 -> 안됨!!

# csv file load
csv_path = "/home/ubuntu/working/spark-examples/data/trips/yellow_tripdata_2021-04.csv"

with open(csv_path) as f:
    reader = csv.reader(f)
    # reader 에서 한줄 씩 불러오기
    for row in reader:
        time.sleep(0.5)
        producer.send(TOPIC_NAME, json.dumps(row).encode("utf-8"))
        print(json.dumps(row).encode("utf-8"))
        # break
producer.flush()
