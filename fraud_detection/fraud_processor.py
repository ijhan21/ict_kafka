# 정상 데이터를 처리하기 위한 컨슈머
from kafka import KafkaConsumer
import json

FRAUD_TOPIC = "fraud_payments"
BROKERS = ["ec2-15-165-103-83.ap-northeast-2.compute.amazonaws.com:9092"]

if __name__=="__main__":
    consumer = KafkaConsumer(FRAUD_TOPIC, bootstrap_servers=BROKERS)

    for message in consumer:
        msg = json.loads(message.value.decode())

        payment_type = msg["PAYMENT_TYPE"]
        payment_date = msg["DATE"]
        payment_time = msg["TIME"]
        amount = msg["AMOUNT"]
        to = msg["TO"]

        print(f"[이상 거래] : {payment_type} {payment_date} {payment_time} << {to} - {amount} >>")