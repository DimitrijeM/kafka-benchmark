import pandas as pd
from consumer import consume_and_count_messages, from_one_topic_to_another

file_path = '/home/dimi/Projects/kafka-benchmark/data/stream.jsonl'
topic_name = 'stream'

print('Hello Kafka')

stat_cac = consume_and_count_messages('in')
df = pd.DataFrame.from_dict(stat_cac)
df.to_csv('/home/dimi/Projects/kafka-benchmark/data/stats.csv', index=False)

stat_foto = from_one_topic_to_another('in', 'out')
df = pd.concat([df, pd.DataFrame.from_dict(stat_foto)], axis=0)
df.to_csv('/home/dimi/Projects/process-engineer/data/stats.csv', index=False)
print('bye Kafka')