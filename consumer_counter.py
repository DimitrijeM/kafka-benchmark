import numpy as np
import ujson
import confluent_kafka
import time

BOOTSTRAP_SERVERS = 'localhost:9092'


def count_unique_users_per_minute(messages):
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


def read_messages_and_count_users(input_topic_name, output_topic_name, group_id='count-group'):
    start = time.time()
    messages = []


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

        message = ujson.loads(message.value().decode('utf-8'))
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
    producer.produce(output_topic_name, value=ujson.dumps(num_of_unique_users).encode('utf-8'))
    producer.flush()
    producer_timing = time.time() - start
    print(len(messages), 'messages have been processed.')
    print('Execution time (seconds): ', consumer_timing)
    return len(messages), consumer_timing, producer_timing





