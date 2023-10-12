import logging
import time
import uuid

import pika
import json


def on_response(ch, method, properties, body):
    """
    Callback function when a response is received. It prints the response and exits.
    """
    print(f"Response: {body.decode()}")
    ch.stop_consuming()


def client(command, body_json):
    """
    Synchronous client to test server.py
    It publishes the message to the exchange created by the Server, waits for the Server's response and exits when
    the response is received.
    Each invocation of client() will have its own exclusive queue to avoid consuming other clients' messages.

    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    exchange_name = 'norbit_exchange'
    channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

    queue_name = f"exclusive_queue_{uuid.uuid4().hex}"
    result = channel.queue_declare(queue=queue_name, exclusive=True)
    callback_queue = result.method.queue

    # Set up the consumer to wait for the server's response:
    channel.basic_consume(queue=callback_queue, on_message_callback=on_response, auto_ack=True)

    channel.basic_publish(
        exchange=exchange_name,
        routing_key=command,
        properties=pika.BasicProperties(reply_to=callback_queue),
        body=json.dumps(body_json).encode('utf-8')
    )

    print(f"Sent '{command, body_json}'")

    channel.start_consuming()


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING)  # set to WARNING if we want a clean output of the response

    command = 'SET'
    body_json = {'key': 'one', 'value': 1}
    client(command, body_json)

    time.sleep(0.1)

    command = 'GET'
    body_json = {'key': 'one'}
    client(command, body_json)

    time.sleep(0.1)

    command = 'GET'
    body_json = {'key': 'dos'}
    client(command, body_json)

    time.sleep(0.1)

    command = 'GET'
    body_json = {'wrong_': 'dos'}
    client(command, body_json)
