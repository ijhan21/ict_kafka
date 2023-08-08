from kafka import KafkaConsumer # 무한 대기가 필요하다
import json
BROCKER_SERVERS = ["localhost:9092"]
TOPIC_NAME = "taxi_pricing_topic"

consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=BROCKER_SERVERS)

print("Wait.........")
# Consumer는 파이썬의 Generator로 구현되어 있다.
for message in consumer:
    # message에서 value를 추출하면 Producer가 보낸 값
    # print(message.value.decode("utf-8"))
    row = json.loads(message.value.decode())
    print(row)
print("Done")