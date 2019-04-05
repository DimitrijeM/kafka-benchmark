from kafka import KafkaConsumer, KafkaProducer
from pykafka import KafkaClient
import confluent_kafka

import json
import time
import random
import string
import numpy as np

GROUP_ID = 'group-random'
BOOTSTRAP_SERVERS = 'localhost:9092'


def count_unique_users_per_minute(messages: list):
    if len(messages) == 0:
        return {}, 0
    messages = np.array(messages)
    start = time.time()
    messages[:, 1] = messages[:, 1].astype('datetime64[m]')
    minutes = np.unique(messages[:, 1])
    num_of_unique_users = {}
    for minute in minutes:
        msgs_in_minute = messages[messages[:,1] == minute]
        num_of_unique_users[minute] = np.unique(msgs_in_minute[:, 0]).shape[0]
    timing = time.time() - start
    print(timing)
    return num_of_unique_users, timing


def consume_and_count_messages(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=1000,
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    arr_of_messages = [] # np.array([])
    start = time.time()

    for message in consumer:
        message = message.value
        # print('{} received.'.format(message))
        arr_of_messages.append(message) # np.append([arr_of_messages], [message])

    consumer.close()
    end = time.time()
    stats = {
        'type': ['consume_and_count'],
        'msg_count': [len(arr_of_messages)],
        'execution_time': [end - start]
    }
    print('Message count:', len(arr_of_messages)) # count #arr_of_messages.shape[0])
    print('Execution time (seconds): ', end-start)
    return stats


def kafka_python_performance(input_topic_name, output_topic_name):
    start = time.time()
    messages = []

    consumer = KafkaConsumer(
        input_topic_name,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=1000,
        group_id=GROUP_ID + '-kp',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    for message in consumer:
        msg = [message.value['uid'], np.datetime64(message.value['ts'], 's')]
        # print('{} received.'.format(message))
        messages.append(msg)
    consumer.close()
    consumer_timing = time.time() - start

    num_of_unique_users = count_unique_users_per_minute(messages)[0]
    print(num_of_unique_users)
    producer.send(output_topic_name, value=num_of_unique_users)
    producer.close()
    producer_timing = time.time() - start

    print(len(messages), 'messages have been processed.')
    print('Execution time (seconds): ', consumer_timing)
    return len(messages), consumer_timing, producer_timing


def pykafka_performance(input_topic_name, output_topic_name, use_rdkafka=False):
    start = time.time()
    client = KafkaClient(hosts=BOOTSTRAP_SERVERS)
    input_topic = client.topics[input_topic_name]

    consumer = input_topic.get_simple_consumer(use_rdkafka=use_rdkafka, consumer_group=GROUP_ID + '-pq',
                                               consumer_timeout_ms=1000)
    messages = []
    while True:
        try:
            message = consumer.consume(block=True)
            message = json.loads(message.value.decode('utf-8'))
        except AttributeError as e:
            print('No more messages', e)
            break

        msg = [message['uid'], np.datetime64(message['ts'], 's')]
        # print('{} received.'.format(message))
        messages.append(msg)
    consumer.stop()
    consumer_timing = time.time() - start

    start = time.time()
    num_of_unique_users = count_unique_users_per_minute(messages)[0]
    print(num_of_unique_users)
    output_topic = client.topics[output_topic_name]
    producer = output_topic.get_producer(use_rdkafka=use_rdkafka)
    producer.produce(json.dumps(num_of_unique_users).encode('utf-8'))
    producer.stop()
    producer_timing = time.time() - start

    print(len(messages), 'messages have been processed.')
    print('Execution time (seconds): ', consumer_timing)
    return len(messages), consumer_timing, producer_timing


def confluent_kafka_performance(input_topic_name, output_topic_name):
    start = time.time()
    messages = []
    group_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

    conf_consumer = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'queue.buffering.max.messages': 500000,
        'queue.buffering.max.ms': 60000,
        'session.timeout.ms': 6000,
        'batch.num.messages': 100,
        'default.topic.config': {
            'auto.offset.reset': 'earliest',
            'acks': 'all'
        }
    }
    consumer = confluent_kafka.Consumer(**conf_consumer)
    consumer.subscribe([input_topic_name])

    while True:
        message = consumer.poll(timeout=4.0)
        if message is None:
            print('Message is None', len(messages))
            break
        if message.error():
            print("Consumer error: {}".format(message.error()))
            continue

        message = json.loads(message.value().decode('utf-8'))
        msg = [message['uid'], np.datetime64(message['ts'], 's')]
        # print(msg, 'received.')
        messages.append(msg)

    consumer.close()
    consumer_timing = time.time() - start

    num_of_unique_users = count_unique_users_per_minute(messages)[0]
    print(num_of_unique_users)

    start = time.time()
    conf_producer = {'bootstrap.servers': BOOTSTRAP_SERVERS}
    producer = confluent_kafka.Producer(**conf_producer)
    producer.poll(0)
    producer.produce(output_topic_name, value=json.dumps(num_of_unique_users).encode('utf-8'))
    producer.flush()
    producer_timing = time.time() - start
    print(len(messages), 'messages have been processed.')
    print('Execution time (seconds): ', consumer_timing)
    return len(messages), consumer_timing, producer_timing

