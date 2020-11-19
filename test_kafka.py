import time
from json import dumps

from kafka import KafkaProducer

import requests

req = requests.get('http://localhost:9090/api/v1/label/__name__/values')
data = req.json()['data']
#
# print(data)

def get_data_to_send():
    data_to_send = dict()
    flag = True
    for d in data:
        if d.split('application')[0] == '' or d.split('process')[0] == '':
            req = requests.get('http://localhost:9090/api/v1/query', params={'query': d})
            metrics = req.json()['data']['result']
            if metrics:
                metrics = metrics[0]
                # print(metrics['metric']['__name__'], metrics['value'])
                if flag:
                    data_to_send['timestamp'] = metrics['value'][0]
                    flag = False

                data_to_send[metrics['metric']['__name__']] = metrics['value'][1]

    return data_to_send

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'),
                         compression_type='gzip')
my_topic = 'metrics'

def process():
    data = get_data_to_send()

    try:
        future = producer.send(topic=my_topic, value=data)
        record_metadata = future.get(timeout=10)

        print('--> The message has been sent to a topic: \
                {}, partition: {}, offset: {}' \
              .format(record_metadata.topic,
                      record_metadata.partition,
                      record_metadata.offset))

    except Exception as e:
        print('--> It seems an Error occurred: {}'.format(e))

    finally:
        producer.flush()


while True:
    process()
    time.sleep(10)
