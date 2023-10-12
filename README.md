# amqp_server
## Simple Key-Value Store Server over AMQP protocol (RabbitMQ) using asyncio.

First, you need to set up RabbitMQ using Docker. Here's a quick way to get RabbitMQ up and running:

```docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:management```

Then, you can run server.py and test that it works using client.py

Dependencies: `aio-pika` and `pika`.

Tested on Python 3.10.
