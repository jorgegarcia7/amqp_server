import json
import asyncio
import logging
import aio_pika


async def process_message(
    message: aio_pika.abc.AbstractIncomingMessage,
    channel: aio_pika.Channel,
    key_value_dict: dict,
) -> None:
    """
    Processes the messages that come from the client and sends back a response.
    Clients can get the value of a key, set the value of a key, and delete a key-value pair. Examples:
    "GET": Example JSON: {"key": "tralala"}
    "SET": Example JSON: {"key": "tralala", "value": 123}
    "DELETE": Example JSON: {"key": "tralala"}

    Note that "delete" will return an error if the key is not present.

    """
    async with message.process():  # to be able to send the acknowledgement of the message
        command = message.routing_key
        data_dict = json.loads(message.body)
        logging.info(f" Received message {command, data_dict}")

        response = {}

        if command == "SET" and set(data_dict.keys()) != {"key", "value"}:
            response["error"] = "Wrong JSON format. Example JSON: {'key': 'tralala', 'value': 123}"

        elif command in ["GET", "DELETE"] and set(data_dict.keys()) != {"key"}:
            response["error"] = "Wrong JSON format. Example JSON: {'key': 'tralala'}"

        else:
            try:
                if command == "GET":
                    response["value"] = key_value_dict[data_dict["key"]]
                elif command == "SET":
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

        response_message = aio_pika.Message(body=json.dumps(response).encode())

        # Publish the response to the reply_to queue specified by the client:
        if message.reply_to:
            logging.info(f" Sending response to {message.reply_to}: {response}")
            await channel.default_exchange.publish(
                response_message, routing_key=message.reply_to
            )

        logging.info(" Done")


async def server() -> None:
    """
    Starts an asynchronous server that acts as a simple key-value store.
    The server handles multiple client connections concurrently using asyncio.

    """
    logging.basicConfig(level=logging.INFO)

    key_value_dict = {}  # In-memory key-value store
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    channel = await connection.channel()

    exchange_name = "norbit_exchange"
    exchange = await channel.declare_exchange(
        exchange_name, aio_pika.ExchangeType.TOPIC
    )
    queue = await channel.declare_queue("all_commands_queue", auto_delete=True)

    # "#" matches any routing_key (works with ExchangeType.TOPIC)
    await queue.bind(exchange, routing_key="#")
    await queue.consume(
        lambda message: process_message(message, channel, key_value_dict)
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
