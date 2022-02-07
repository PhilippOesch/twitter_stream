import json

from kafka import KafkaProducer


class TwitterProducer:
    def __init__(self, kafka_server: str):
        self.twitter_producer: KafkaProducer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def send(self, topic: str, tweet_id, data: dict) -> None:
        self.twitter_producer.send(topic, value=data, key=tweet_id)

    def flush(self):
        self.twitter_producer.flush()
