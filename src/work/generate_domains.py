import threading
import pika
import datetime
import json
import validators
import whois
import pandas as pd
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


def main():
    obj = DB(DB_NAME, DB_USER, DB_PASS, DB_HOST, DB_PORT)
    try:
        obj.initilizeTable()
    except UnboundLocalError as err:
        print("Please check PostgreSQL connection credentials.", err)
    credentials = pika.PlainCredentials(RBMQ_USER, RBMQ_PASS)
    parameters = pika.ConnectionParameters(RBMQ_HOST, RBMQ_PORT, '/', credentials=credentials)
    connection = pika.BlockingConnection(parameters)

    channel = connection.channel()

    channel.exchange_declare(exchange='workers', exchange_type='direct')

    channel.queue_declare(queue='worker_queue', durable=True)

    channel.queue_bind(exchange='workers', queue="worker_queue")

    df = pd.read_csv('domains.csv')
    return df, channel


def data(df, channel):
    # Creating defaukt date, because in some domains
    # EXPIRATION_DATE and CREATION_DATE not a date format.
    default_date = '1111-11-11 11:11:11'
    for index, row in df.iterrows():
        host = row['Domain']
        if not validators.domain(host):
            message = {
                "ID": index,
                "DOMAIN": host,
                "DOAMIN_STATUS": False,
                "EXPIRATION_DATE": default_date,
                "CREATION_DATE": default_date,
                "COUNTRY": '',
                "NAME": '',
                "ORG": '',
                "ADDRESS": '',
                "CITY": '',
                "STATE": '',
                "ZIPCODE": 0
            }
        else:
            try:
                res = whois.whois(host)
            except Exception as err:
                message = {
                    "ID": index,
                    "DOMAIN": host,
                    "DOAMIN_STATUS": False,
                    "EXPIRATION_DATE": default_date,
                    "CREATION_DATE": default_date,
                    "COUNTRY": '',
                    "NAME": '',
                    "ORG": '',
                    "ADDRESS": 'Redirecting',
                    "CITY": '',
                    "STATE": '',
                    "ZIPCODE": 0
                }
                channel.basic_publish(
                    exchange='workers',
                    routing_key='worker_queue',
                    body=json.dumps(message),
                    properties=pika.BasicProperties(
                        delivery_mode=2,))
                continue
            exp_date = res.expiration_date
            if isinstance(exp_date, list):
                exp_date = exp_date[0]
            elif not isinstance(exp_date, datetime.datetime):
                exp_date = default_date
            create_date = res.creation_date
            if isinstance(create_date, list):
                create_date = create_date[0]
            elif not isinstance(create_date, datetime.datetime):
                create_date = default_date
            name = res.name
            org = res.org
            address = res.address
            city = res.city
            state = res.state
            zipcode = res.zipcode
            if zipcode is None or not isinstance(zipcode, int):
                zipcode = 0
            country = res.country
            message = {
                "ID": index,
                "DOMAIN": host,
                "DOAMIN_STATUS": True,
                "EXPIRATION_DATE": str(exp_date),
                "CREATION_DATE": str(create_date),
                "COUNTRY": country,
                "NAME": name,
                "ORG": org,
                "ADDRESS": address,
                "CITY": city,
                "STATE": state,
                "ZIPCODE": zipcode
            }
        channel.basic_publish(
            exchange='workers',
            routing_key='worker_queue',
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,))
        print("[i] Domain id=%s, name is %s has Sent." % (index, host))


if __name__ == "__main__":
    df, channel = main()
    df1 = df[:len(df)//4]
    df2 = df[len(df)//4:len(df)//2]

    df1 = df[:len(df)//2]
    df2 = df[len(df)//2:]

    thread1 = threading.Thread(target=data(df1, channel))
    thread2 = threading.Thread(target=data(df2, channel))

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()
