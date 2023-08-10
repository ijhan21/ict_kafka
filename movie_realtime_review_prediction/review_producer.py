from kafka import KafkaProducer
import pandas as pd
import urllib.request
import json
import time

TOPIC_NAME = "naver_reviews"

# 브로커 설정
BROKERS = ["ec2-15-165-103-83.ap-northeast-2.compute.amazonaws.com:9092"]

producer = KafkaProducer(bootstrap_servers=BROKERS)
urllib.request.urlretrieve("https://raw.githubusercontent.com/e9t/nsmc/master/ratings_test.txt", filename="ratings_test.txt")
df_test  = pd.read_table('ratings_test.txt')
# print(df_test.head())

for sentence in df_test['document']:
    msg = {
        "sentence":sentence
    }

    producer.send(TOPIC_NAME, json.dumps(msg).encode("utf-8"))
    print(msg)
    time.sleep(1)

    producer.flush()