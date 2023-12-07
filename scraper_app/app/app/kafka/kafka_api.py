import json

from confluent_kafka import Producer

from app.core.config import settings


class KafkaApi:
    def __init__(self, logger=None, topic=None):
        if settings.USE_KAFKA and settings.KAFKA_USERNAME and settings.KAFKA_PASSWORD:
            self.producer = Producer({'bootstrap.servers': settings.KAFKA_SERVER,
                                      'security.protocol': 'sasl_plaintext',
                                      'sasl.mechanism': 'PLAIN',
                                      'sasl.username': settings.KAFKA_USERNAME,
                                      'sasl.password': settings.KAFKA_PASSWORD
                                      })
        else:
            self.producer = None
        self.logger = logger
        self.counter = 0
        self.topic = topic

    def send_to_kafka(self, users_dict):
        if settings.USE_KAFKA:
            users_lst = [users_dict[x] for x in list(users_dict.keys())]
            if self.producer:
                for user in users_lst:
                    self.counter += 1
                    self.producer.produce(self.topic, json.dumps(user, default=str).encode('utf-8'),
                                          callback=self.delivery_report)
                if self.counter > 1000:
                    self.flush_messages()
                    self.counter = 0

    def flush_messages(self):
        if self.producer:
            self.producer.flush()
            self.logger.info("Messages flushed")

    def delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            self.logger.error('Message delivery failed: {}'.format(err))
        else:
            pass
            # self.logger.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
