import logging
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
    The client implements a timeout, so if no response is received by the server then it exits.

    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()

    exchange_name = "norbit_exchange"
    channel.exchange_declare(exchange=exchange_name, exchange_type="topic")

    queue_name = f"exclusive_queue_{uuid.uuid4().hex}"
    result = channel.queue_declare(queue=queue_name, exclusive=True)
    callback_queue = result.method.queue

    # Set up the consumer to wait for the server's response:
    channel.basic_consume(
        queue=callback_queue, on_message_callback=on_response, auto_ack=True
    )

    channel.basic_publish(
        exchange=exchange_name,
        routing_key=command,
        properties=pika.BasicProperties(reply_to=callback_queue),
        body=json.dumps(body_json).encode("utf-8"),
    )

    print(f"Sent '{command, body_json}'")

    # Using a loop with channel.consume() instead of channel.start_consuming() to be able to implement an
    # inactivity_timeout.
    timeout_seconds = 5
    for method_frame, properties, body in channel.consume(
        callback_queue, inactivity_timeout=timeout_seconds
    ):
        if method_frame:
            on_response(channel, method_frame, properties, body)
            break
        else:
            print("Request timed out!")
            break


if __name__ == "__main__":
    # set logging to DEBUG/INFO if we want to see more details
    logging.basicConfig(level=logging.WARNING)

    client("SET", {"key": "one", "value": 1})

    client("GET", {"key": "one"})

    client("GET", {"key": "two"})

    client("SET", {"key": "two", "value": "2"})

    client("GET", {"key": "two"})

    client("GET", {"wrong_": "two"})

    client("DELETE", {"key": "one"})

    client("GET", {"key": "one"})

    client("DELETE", {"key": "one"})

    client("DELETE_wrong", {"key": "one"})
