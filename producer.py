import time
import json
from kafka import KafkaProducer


def send_messages_from_file_to_kafka(file_path, topic_name):
    start = time.time()
    arr_of_messages = []

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    with open(file_path) as f:
        # c = 0
        for line in f:
            # c = c+1
            message = json.loads(line)
            producer.send(topic_name, message)
            arr_of_messages.append(message)
            # if c == 5000:
            #     break
    end = time.time()
    print(arr_of_messages.shape[0], ' messages pushed to Kafka topic ',
          topic_name, '. Execution time (seconds): ', end-start)

    stats = {
        'type': ['push_message_from_file'],
        'msg_count': [len(arr_of_messages)],
        'execution_time': [end - start]
    }
    return stats


def send_message_to_kafka(messages, topic_name):
    start = time.time()

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    for msg in messages:
        producer.send(topic_name, value=msg)

    end = time.time()
    print(len(messages), 'messages pushed to Kafka topic',
          topic_name, '; Execution time (seconds):', end - start)

    stats = {
        'type': ['push_messages'],
        'msg_count': [len(messages)],
        'execution_time': [end - start]
    }
    return stats



