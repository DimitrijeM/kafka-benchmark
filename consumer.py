from kafka import KafkaConsumer
import json
import time
from kafka import KafkaProducer

GROUP_ID = 'count-group-1'


def consume_and_count_messages(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=1000,
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    arr_of_messages = [] # np.array([])
    # count = 0
    start = time.time()

    for message in consumer:
        # count = count + 1
        message = message.value
        print('{} received.'.format(message))
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


def from_one_topic_to_another(input_topic_name, output_topic_name):
    consumer = KafkaConsumer(
        input_topic_name,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=1000,
        group_id=GROUP_ID+'-1',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    arr_of_messages = [] # np.array([])
    # count = 0
    start = time.time()
    print(start)
    for message in consumer:
        # count = count + 1
        message = message.value
        print('{} received.'.format(message))
        producer.send(output_topic_name, value=message)
        arr_of_messages.append(message) # np.append([arr_of_messages], [message])

    consumer.close()
    producer.close()
    end = time.time()

    stats = {
        'type': ['from_one_topic_to_another'],
        'msg_count': [len(arr_of_messages)],
        'execution_time': [end - start]
    }
    print('Message count:', len(arr_of_messages))  # count #arr_of_messages.shape[0])
    print('Execution time (seconds): ', end - start)

    return stats

