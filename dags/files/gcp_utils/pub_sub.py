import time

from google.cloud import pubsub_v1
import os


def push_message(topic, message):
    publisher = pubsub_v1.PublisherClient()
    topic_name = f'projects/poc-prj-001-422609/topics/{topic}'
    # publisher.create_topic(name=topic_name)
    future = publisher.publish(topic_name, message.encode('utf-8'), spam='eggs')
    return future.result()


# print(push_message('new_sales_data', f'test_message - {time.time()}'))