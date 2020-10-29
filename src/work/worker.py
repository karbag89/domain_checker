import pika
import time
import json
from database.db import DB

"""
PostgreSQL Database connection credentials.
"""
# TODO -> Write appropriate credentials for connecting to PostgreSQL Database.
DB_NAME = "****"
DB_USER = "****"
DB_PASS = "****"
DB_HOST = "****"
DB_PORT = "0000"

"""
RabbitMQ connection credentials.
"""
# TODO -> Write appropriate credentials for connecting to RabbitMQ.
RBMQ_USER = "****"
RBMQ_PASS = "****"
RBMQ_HOST = "****"
RBMQ_PORT = 0000


def callback(ch, method, properties, body):
    message = json.loads(body)

    if message['DOAMIN_STATUS']:
        status = 'VALID'
    else:
        status = 'INVALID'
    print("[i] Domain id=%s, name is %s, status %s." % (message["ID"],
                                                        message["DOMAIN"],
                                                        status))

    time.sleep(0.3)
    data = list(message.values())
    obj1 = DB(DB_NAME, DB_USER, DB_PASS, DB_HOST, DB_PORT)
    obj1.writeDataToTabel(data)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    obj = DB(DB_NAME, DB_USER, DB_PASS, DB_HOST, DB_PORT)
    try:
        obj.initilizeTable()
    except UnboundLocalError as err:
        print("Please check PostgreSQL connection credentials.", err)
    credentials = pika.PlainCredentials(RBMQ_USER, RBMQ_PASS)
    parameters = pika.ConnectionParameters(RBMQ_HOST, RBMQ_PORT, '/',
                                           credentials)
    connection = pika.BlockingConnection(parameters)

    channel = connection.channel()

    channel.queue_declare(queue='worker_queue', durable=True)
    print("[*] Waiting for messages. To exit press CTRL+C")

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='worker_queue', on_message_callback=callback)

    channel.start_consuming()


if __name__ == "__main__":
    main()
