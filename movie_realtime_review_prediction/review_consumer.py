from kafka import KafkaConsumer
import json

# pickle로 저장된 모델 파일을 불러오기 위해 import
from joblib import load

TOPIC_NAME = "naver_reviews"
BROKERS = ["ec2-15-165-103-83.ap-northeast-2.compute.amazonaws.com:9092"]

consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=BROKERS)

# tfidfvectorizer, model 파일 불러오기
tfidf_vectorizer = load("tfidf_vectorizer.pkl")
model = load('korean_model.pkl')

for msg in consumer:
    sentence = json.loads(msg.value.decode())['sentence']
    # 벡터화
    test_vector = tfidf_vectorizer.transform([sentence])
    prediction = model.predict(test_vector)[0]
    print(sentence, '=====>', prediction)
print("Done.....")