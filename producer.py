from kafka import KafkaProducer

# 브로커 목록 정의하기.
# - 브로커가 한 개만 있다 하더라도 리스트 형태로 정의하는 것이 좋다.
# - 여러개의 브로커가 띄워진 경우에도 한 개의 브로커만 입력하는게 아닌, 모든 브로커를 다 적는 것이 좋다.

BROCKER_SERVERS = ["localhost:9092"]
TOPIC_NAME = "sample_topic"

# 프로듀서 생성
producer = KafkaProducer(bootstrap_servers=BROCKER_SERVERS)

# 메시지를 토픽에 전송 - send 메소드 활용
producer.send(TOPIC_NAME, b"Hello Kafka Python")

# 버퍼 플러싱
producer.flush()

# 버퍼로 데이터를 옮기다 보면 찌꺼기 같은게 남는 경우가 있다. 이러한 것들을 한꺼번에 밀어내는 것을 flush
# 없어도 작동하는데, 오류 방지의 의미가 크다. 대기중인 찌꺼기가 미처 못가서 멈추는 현상 등이 가능함.