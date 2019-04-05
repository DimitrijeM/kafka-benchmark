import pandas as pd
import matplotlib.pyplot as plt
import os
from kafka_clients_performance import kafka_python_performance, pykafka_performance, confluent_kafka_performance
from consumer_counter import read_messages_and_count_users
import random
import string


def benchmark_kafka_clients(input_topic, output_topic, msg_size, stats_file_path, fig_path):
    num_of_messages, consumer_timings, producer_timings = {}, {}, {}
    num_of_messages['kafka_python'], consumer_timings['kafka_python'], producer_timings[
        'kafka_python'] = kafka_python_performance(input_topic, output_topic)
    num_of_messages['pykafka'], consumer_timings['pykafka'], producer_timings[
        'pykafka'] = pykafka_performance(input_topic, output_topic)
    num_of_messages['confluent_kafka'], consumer_timings['confluent_kafka'], producer_timings[
        'confluent_kafka'] = confluent_kafka_performance(input_topic, output_topic)

    df = pd.DataFrame.from_dict(consumer_timings, orient='index').rename(columns={0: 'time_in_seconds'})
    df['consumed_Bs/s'] = msg_size / df.time_in_seconds
    df['consumed_Frames/s'] = list(num_of_messages.values()) / df.time_in_seconds
    df['producer_time'] = list(producer_timings.values())
    df.sort_index(inplace=True)

    df.plot(kind='bar', subplots=True, figsize=(10, 8), title="Comparison")

    df.to_csv(stats_file_path)
    plt.savefig(fig_path)
    print('Results was saved in:')
    print('- data: ', stats_file_path)
    print('- graph: ', fig_path)


if __name__ == '__main__':
    path = os.path.dirname(os.path.abspath(__file__))
    file_path = path + '/data/stream.jsonl'
    stats_file_path = path + '/data/comparison_stats.csv'
    input_topic = 'in'
    output_topic = 'outkp'
    fig_path = path + '/figs/comparison_graph.pdf'
    msg_count = 10 ** 6

    file_path = path + '/data/stream.jsonl'
    stats_file_path = path + 'data/clients_comparison_stats.csv'
    fig_path = path + 'figs/comparison_graph.pdf'

    msg_size = os.path.getsize(file_path)

    print('Hello TDA team, welcome!')
    # benchmark_kafka_clients(input_topic, output_topic, msg_size, stats_file_path, fig_path)
    group_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

    read_messages_and_count_users(input_topic, output_topic, group_id)
