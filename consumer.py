from kafka import KafkaConsumer # 무한 대기가 필요하다

BROCKER_SERVERS = ["localhost:9092"]
TOPIC_NAME = "sample_topic"

consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=BROCKER_SERVERS)

print("Wait.........")
# Consumer는 파이썬의 Generator로 구현되어 있다.
for message in consumer:
    print(message)
print("Done")