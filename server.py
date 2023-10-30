import json
import asyncio
import logging
import aio_pika


async def execute_command(command, data_dict, key_value_dict):
    """
    Execute the given command and return the response.
    Clients can get the value of a key, set the value of a key, and delete a key-value pair. Examples:
    "GET": Example JSON: {"key": "tralala"}
    "SET": Example JSON: {"key": "tralala", "value": 123}
    "DELETE": Example JSON: {"key": "tralala"}

    Note that "delete" will return an error if the key is not present.

    """
    response = {}

    if command == "SET" and set(data_dict.keys()) != {"key", "value"}:
        response["error"] = "Wrong JSON format. Example JSON: {'key': 'tralala', 'value': 123}"

    elif command in ["GET", "DELETE"] and set(data_dict.keys()) != {"key"}:
        response["error"] = "Wrong JSON format. Example JSON: {'key': 'tralala'}"

    else:
        try:
            if command == "GET":
                await asyncio.sleep(1)  # simulating "GET" commands take 1 second of I/O operation
                response["value"] = key_value_dict[data_dict["key"]]
            elif command == "SET":
                await asyncio.sleep(5)  # simulating "SET" commands take 5 seconds of I/O operation
                key_value_dict[data_dict["key"]] = data_dict["value"]
                response["status"] = "OK"
            elif command == "DELETE":
                del key_value_dict[data_dict["key"]]
                response["status"] = "OK"
            else:
                response["error"] = f"Invalid command: {command}"
        except KeyError as e:
            response["error"] = f"Key error: {str(e)}"
        except Exception as e:
            response["error"] = str(e)

    return response


async def process_message(message, channel, key_value_dict):
    """
    Processes the message that comes from the client and sends back a response.

    """
    # Acknowledge the message immediately:
    await message.ack()

    command = message.routing_key
    data_dict = json.loads(message.body)
    corr_id = message.correlation_id
    logging.info(f" Received message {command, data_dict}")

    response = await execute_command(command, data_dict, key_value_dict)
    response_message = aio_pika.Message(
        body=json.dumps(response).encode(), correlation_id=corr_id
    )

    # Publish the response to the reply_to queue specified by the client:
    if message.reply_to:
        logging.info(f" Sending response to {message.reply_to}: {response}")
        await channel.default_exchange.publish(
            response_message, routing_key=message.reply_to
        )

    logging.info(" Done")


async def consume_message(message, channel, key_value_dict):
    """
    Create a separate asynchronous task for each message to be processed concurrently.
    """
    asyncio.create_task(process_message(message, channel, key_value_dict))


async def server():
    """
    Starts an asynchronous server that acts as a simple key-value store.
    The server handles multiple client connections concurrently using asyncio.

    """
    logging.basicConfig(level=logging.INFO)

    key_value_dict = {}  # In-memory key-value store
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    channel = await connection.channel()

    # Forcing prefetch_count=1 to test that the server is truly asynchronous:
    await channel.set_qos(prefetch_count=1)

    exchange_name = "norbit_exchange"
    exchange = await channel.declare_exchange(
        exchange_name, aio_pika.ExchangeType.TOPIC
    )
    queue = await channel.declare_queue("all_commands_queue", auto_delete=True)

    # "#" matches any routing_key (works with ExchangeType.TOPIC)
    await queue.bind(exchange, routing_key="#")
    await queue.consume(
        lambda message: consume_message(message, channel, key_value_dict)
    )

    try:
        logging.info(" Waiting for clients...")
        # Run in perpetuity until it is interrupted (for example with SIGINT)
        await asyncio.Future()
    finally:
        logging.info(" Closing connection...")
        await connection.close()


if __name__ == "__main__":
    asyncio.run(server())
