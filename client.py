import logging
import os
import time
import uuid
from concurrent import futures

import pika
import json


pending_responses = {}


def on_response(ch, method, properties, body):
    """
    Callback function when a response is received. It prints the response and exits if no there are no pending
    commands to be responded.
    """
    if properties.correlation_id in pending_responses:
        print(f"Response: {body.decode()}. Correlation_id: {properties.correlation_id}")
        del pending_responses[properties.correlation_id]

        if not pending_responses:
            ch.stop_consuming()


def client(commands):
    """
    Synchronous client to test server.py
    It publishes the commands to the exchange created by the Server, waits for the Server's response and exits when
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

    for message in commands:
        command = message[0]
        body_json = message[1]

        corr_id = str(uuid.uuid4())
        pending_responses[corr_id] = None

        channel.basic_publish(
            exchange=exchange_name,
            routing_key=command,
            properties=pika.BasicProperties(
                reply_to=callback_queue, correlation_id=corr_id
            ),
            body=json.dumps(body_json).encode("utf-8"),
        )

        print(f"Sent '{command, body_json}. Correlation_id: {corr_id}'")

    # Using a loop with channel.consume() instead of channel.start_consuming() to be able to implement an
    # inactivity_timeout.
    timeout_seconds = 60
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

    start_time = time.time()

    messages = (("SET", {"key": "one", "value": 1}), ("GET", {"key": "one"}))
    processes = os.cpu_count()

    _process_jobs = []
    with futures.ProcessPoolExecutor(max_workers=processes) as p:
        for i in range(10):
            job = p.submit(client, messages)
            _process_jobs.append(job)

    for job in futures.as_completed(_process_jobs):
        job.result()  # wait for result

    end_time = time.time()
    logging.warning(f'It took {end_time - start_time} seconds to execute all the commands.')
